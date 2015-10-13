"""File tree history with hardlinks."""
import collections
import logging
import os
import shutil
import re
import datetime

_logger = logging.getLogger(__name__)

QUEUE_NAME_PATTERN = re.compile(r'^\w+$')
BACKUP_DIR_NAME_PATTERN = re.compile(r'^(?P<queue>\w+)-(?P<timestamp>\d{4}\d{2}\d{2}\d{2}\d{2}\d{2})$')


class BackupQueueSpecError(Exception): pass


class SourceDirError(Exception): pass


class DestinationDirError(Exception): pass


#: Specification of a backup queue by its name, the minimum age of the last backup and the number of backups in this queue.
BackupQueueSpec = collections.namedtuple('BackupQueueSpec', ['name', 'age', 'length'])

#: Existing backup on disk.
Backup = collections.namedtuple('Backup', ['name', 'queue', 'timestamp'])


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
        self.backups = []

    def backup(self):
        """Main driver of the folder backup operation."""

        self._verify_queue_specs()
        self._prepare_directories()
        self._find_backups()
        if self.srcdir_modified:
            self._link_source()
            self._update_queues()

    @property
    def srcdir_modified(self):
        """Checks if the source directory has any file that is newer than the latest backup."""

        # if there is nothing to compare to, we are always out of date:
        if not self.backups:
            return True

        # get latest timestamp of existing backups:
        timestamp_backup = self._get_backup_time()
        _logger.debug('Timestamp of backup: {0}'.format(timestamp_backup))

        # get the latest modification time of the directory tree:
        for dirpath, dirnames, filenames in os.walk(self.srcdir):
            # only interested in files:
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                timestamp_file = datetime.datetime.fromtimestamp(os.path.getmtime(filepath))
                if timestamp_file > timestamp_backup:
                    _logger.debug('Source dir contains a newer file, updated at {0}: {1}'.format(timestamp_file, filepath))
                    return True

        _logger.debug('No newer file found.')
        return False

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

        _logger.info('Creating hard-linked snapshot of source directory.')
        linked_dirname = ''

    def _find_backups(self):
        """Reads current backups from filesystem."""

        for entry in os.listdir(self.dstdir):
            if os.path.isdir(os.path.join(self.dstdir, entry)):
                m = BACKUP_DIR_NAME_PATTERN.match(entry)
                if m:
                    backup = Backup(entry, *m.groups())
                    _logger.debug('Found existing backup {0}.'.format(str(backup)))
                    self.backups.append(backup)

        # sort by timestamp from newest to oldest backup:
        self.backups = sorted(self.backups, key=lambda b: b.timestamp, reverse=True)

        if self.backups:
            _logger.info('Found {0} backups with latest modification timestamp {1}.'.format(len(self.backups), self.backups[0]))
        else:
            _logger.info('No previous backups found.')

    def _update_queues(self):
        pass

    def _get_backup_time(self):
        """Returns newest backup's timestamp."""
        timestamp_text = self.backups[0].timestamp
        year = int(timestamp_text[0:4])
        month = int(timestamp_text[4:6])
        day = int(timestamp_text[6:8])
        hour = int(timestamp_text[8:10])
        minute = int(timestamp_text[10:12])
        return datetime.datetime(year, month, day, hour, minute)
