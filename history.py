"""File tree history with hardlinks."""
import collections
import logging
import os
import shutil
import re
import datetime

_logger = logging.getLogger(__name__)

QUEUE_NAME_PATTERN = re.compile(r'^\w+$')
BACKUP_DIR_NAME_PATTERN = re.compile(r'''
    ^
    (?P<queue>
        \w+             # Queue name
    )
    -
    (?P<timestamptext>
        \d{14}          # Year, Month, Day, Hour, Minute, Second
    )
    $
''', re.VERBOSE)


class BackupQueueSpecError(Exception): pass


class SourceDirError(Exception): pass


class DestinationDirError(Exception): pass


#: Specification of a backup queue by its name, the minimum age of the last backup and the number of backups in this queue.
BackupQueueSpec = collections.namedtuple('BackupQueueSpec', ['name', 'age', 'length'])

#: Existing backup on disk.
Backup = collections.namedtuple('Backup', ['name', 'queue', 'time'])


class BackupTime:
    """Timestamp of a backup folder, resolution of one second."""

    #: Length of timestamp text representation.
    TEXT_LEN = 14

    def __init__(self, stamp):
        assert isinstance(stamp, datetime.datetime)
        self.stamp = stamp

    def __repr__(self):
        return '{ts:%Y%m%d%H%M%S}'.format(ts=self.stamp)

    @property
    def text(self):
        return str(self)

    @classmethod
    def fromtext(cls, text):
        """Creates new instance from text representation."""
        if not text or len(text) != cls.TEXT_LEN:
            raise ValueError('text representation {} has unexpected format'.format(text))

        year = int(text[0:4])
        month = int(text[4:6])
        day = int(text[6:8])
        hour = int(text[8:10])
        minute = int(text[10:12])
        second = int(text[12:14])

        return cls(datetime.datetime(year, month, day, hour, minute, second))

    def __gt__(self, other):
        return self.stamp > other.stamp

    def __eq__(self, other):
        return self.stamp == other.stamp


class FolderHistory:
    """History of a folder via hardlinks, assuming the source files are _not_ changed in-place.

    Typical use is to hold a history of an rsync target folder, if rsync's --inplace flag is _not_ used. In that case, rsync writes to a temporary file during
    synchronization and then moves this file, i.e. a new inode entry is created and existing hardlinks will point to the previous version of the changed file.

    The script writes to the destination folder but affects only directories that match the expected naming pattern.

    A new backup enters the daily queue, if the last backup was done a day or longer ago (but not, if it was just 5 minutes ago). If `numdays` backups existed in this
    queue the oldest is a candidate for the
    """

    def __init__(self, srcdir, dstdir, queue_specs):
        #: Source directory that is backed up.
        self.srcdir = srcdir
        #: Root of the directory, where the backups are stored.
        self.dstdir = dstdir
        #: Specifications of backup queues.
        self.queue_specs = queue_specs

        #: List of existing backup directories in dstdir.
        self.backups = None

        self._srcdir_timestamp = None

    def backup(self):
        """Main driver of the folder backup operation."""

        self._verify_queue_specs()
        self._prepare_directories()
        self._find_backups()
        if self.srcdir_updated:
            linked_dir = self._link_source()
            self._update_queues()

    @property
    def backup_timestamp(self):
        """Timestamp of newest backup."""

        if self.backups is None:
            self._find_backups()

        if self.backups:
            return self.backups[0].time.stamp
        else:
            return None

    @property
    def srcdir_timestamp(self):
        """Timestamp of newest file in source directory."""

        if not self._srcdir_timestamp:
            # get the latest modification time of the directory tree:
            for dirpath, dirnames, filenames in os.walk(self.srcdir):
                # only interested in files:
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    timestamp_file = datetime.datetime.fromtimestamp(os.path.getmtime(filepath))

                    # round off to secs:
                    timestamp_file = datetime.datetime(year=timestamp_file.year,
                                                       month=timestamp_file.month,
                                                       day=timestamp_file.day,
                                                       hour=timestamp_file.hour,
                                                       minute=timestamp_file.minute,
                                                       second=timestamp_file.second)

                    if not self._srcdir_timestamp or timestamp_file > self._srcdir_timestamp:
                        _logger.debug('Source dir contains a file, updated at {0}: {1}'.format(timestamp_file, filepath))
                    self._srcdir_timestamp = timestamp_file

        return self._srcdir_timestamp

    @property
    def srcdir_updated(self):
        """Flag whether a file in the source dir has a newer timestamp than the last backup."""
        return not self.backup_timestamp or self.backup_timestamp < self.srcdir_timestamp

    def _verify_queue_specs(self):
        """Ensures the specification of the backup queues are consistent and reasonable."""

        if not self.queue_specs:
            raise BackupQueueSpecError('queue_specs must be specified')

        passthrough = 1
        for spec in self.queue_specs:
            if not QUEUE_NAME_PATTERN.match(spec.name):
                raise BackupQueueSpecError('Queue name does not match prescribed regex pattern "{0}".'.format(QUEUE_NAME_PATTERN.pattern))

            if not spec.age >= passthrough:
                raise BackupQueueSpecError('Queue %s has minimum age of {0} days, but backups already take {1} days '
                                           'to get through previous queue.'.format(spec.name, spec.age, passthrough))
            passthrough = spec.age * spec.length

        if not passthrough > 0:
            raise BackupQueueSpecError('Last queue has invalid passthrough time of {0} days.'.format(passthrough))

    def _prepare_directories(self):
        """Checks directories used as input and output and creates output folder on demand."""

        if not os.path.exists(self.srcdir) or not os.path.isdir(self.srcdir):
            raise SourceDirError('{0} is not a valid directory to backup from.'.format(self.srcdir))
        if os.path.exists(self.dstdir):
            if not os.path.isdir(self.dstdir):
                raise DestinationDirError('{0} exists but is not a valid directory to backup to.'.format(self.dstdir))
        else:
            _logger.info('Creating output directory {0}.'.format(self.dstdir))
            os.makedirs(self.dstdir)

    def _link_source(self):
        """Hard-links source to a new timestamped directory in destination."""

        linked_dirname = '{queue}-{ts:%Y%m%d%H%M%S}'.format(queue=self.queue_specs[0].name, ts=self.srcdir_timestamp)
        _logger.info('Creating hard-linked snapshot of source directory in backup directory {}.'.format(linked_dirname))

        linked_dirpath = os.path.join(self.dstdir, linked_dirname)

        try:
            shutil.copytree(self.srcdir, linked_dirpath, copy_function=os.link)
            _logger.info('Hard-linked copy complete.')

            # remember successful backup:
            self.backups.insert(0, Backup(linked_dirname, self.queue_specs[0].name, BackupTime(self.srcdir_timestamp)))
        except shutil.Error as e:
            _logger.error('Creation of hard-linked tree copy failed: {}'.format(e))
            _logger.debug('Trying to clean up invalid copy.')
            shutil.rmtree(linked_dirpath, ignore_errors=True)

    def _find_backups(self):
        """Reads current backups from filesystem."""

        self.backups = []

        for entry in os.listdir(self.dstdir):
            if os.path.isdir(os.path.join(self.dstdir, entry)):
                m = BACKUP_DIR_NAME_PATTERN.match(entry)
                if m:
                    backup = Backup(entry, queue=m.group('queue'), time=BackupTime.fromtext(m.group('timestamptext')))
                    _logger.debug('Found existing backup {0}.'.format(str(backup)))
                    self.backups.append(backup)

        # sort by timestamp from newest to oldest backup:
        self.backups = sorted(self.backups, key=lambda b: b.time, reverse=True)

        if self.backups:
            _logger.info('Found {0} backups with latest modification timestamp {1}.'.format(len(self.backups), self.backups[0]))
        else:
            _logger.info('No previous backups found.')

    def _update_queues(self):
        pass
