"""Single snapshot."""
import datetime
import logging
import os
import re
import shutil
from exceptions import SnapshotDirError, SourceDirError, DestinationDirError, QueueSpecError

_logger = logging.getLogger(__name__)

SNAPSHOT_NAME_PATTERN = re.compile(r'^(?P<queue>\w+)-(?P<timestamptext>\d{14})$')


class Snapshot:
    """Single snapshot of source folder."""

    def __init__(self, dirpath):
        self.dirpath = dirpath
        self.name = os.path.basename(self.dirpath)

        if not os.path.exists(dirpath):
            raise SnapshotDirError('Snapshot directory {} does not exist.'.format(dirpath))

        m = SNAPSHOT_NAME_PATTERN.match(self.name)
        if not m:
            raise SnapshotDirError('Snapshot directory name {} does not match naming pattern.'.format(self.name))

        self.queue_name = m.group('queue')

        timestamp_text = m.group('timestamptext')
        year = int(timestamp_text[0:4])
        month = int(timestamp_text[4:6])
        day = int(timestamp_text[6:8])
        hour = int(timestamp_text[8:10])
        minute = int(timestamp_text[10:12])
        second = int(timestamp_text[12:14])

        self.time = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second)

    def __str__(self):
        return self.name


class Queue:
    """Ordered list of snapshots.

    :ivar name: Name of the queue.
    :ivar delta: Number of days between two snapshots in this queue.
    :ivar length: Number of snapshots in consolidated queue.
    :ivar snapshots: List of snapshots currently in queue.
    """

    def __init__(self, name, delta, length):
        self.name = name
        self.delta = delta
        self.length = length

        self.snapshots = None


class Organizer:
    """Management of snapshot queues.

    :ivar srcdir: Path to source directory of which snapshots are managed.
    :ivar dstdir: Path to directory where snapshot folders are stored.
    :ivar queues: Snapshot queues to be managed.
    """

    def __init__(self, srcdir, dstdir, queues):
        self.srcdir = srcdir
        self.dstdir = dstdir
        self.queues = queues

        self.queue_by_name = {q.name: q for q in self.queues}

        if not os.path.exists(srcdir):
            raise SourceDirError('Source directory {} does not exist.'.format(srcdir))
        if not os.path.exists(dstdir):
            raise DestinationDirError('Destination directory does not exist.'.format(dstdir))
        if not queues:
            raise QueueSpecError('No snapshot queues defined.')

    @property
    def srcdir_time(self):
        """Time of newest file in source directory."""

        # get the latest modification time of the directory tree:
        newest_time = datetime.datetime.fromtimestamp(os.path.getmtime(self.srcdir))

        for dirpath, _, filenames in os.walk(self.srcdir):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                time = datetime.datetime.fromtimestamp(os.path.getmtime(filepath))
                newest_time = max(time, newest_time)

        return self.rounded_to_seconds(newest_time)

    @property
    def snapshots_time(self):
        """Time of newest snapshot in queues."""

        queue = self.queues[0]
        return queue.snapshots[0].time if queue.snapshots else None

    @classmethod
    def rounded_to_seconds(cls, time):
        return datetime.datetime(year=time.year,
                                 month=time.month,
                                 day=time.day,
                                 hour=time.hour,
                                 minute=time.minute,
                                 second=time.second)

    def find_snapshots(self):
        """Detects valid snapshot folders in destination directory."""

        for queue in self.queues:
            queue.snapshots = []

        for entry in os.listdir(self.dstdir):
            fullpath = os.path.join(self.dstdir, entry)
            if os.path.isdir(fullpath):
                m = SNAPSHOT_NAME_PATTERN.match(entry)
                if m:
                    snapshot = Snapshot(fullpath)
                    queue = self.queue_by_name.get(snapshot.queue_name)
                    if queue:
                        _logger.debug('Adding snapshot {s} to quote {q}.'.format(s=snapshot.name, q=queue.name))
                        queue.snapshots.append(snapshot)
                    else:
                        _logger.warning(
                            'Snapshot {s} cannot be mapped to any of these queues: {qs}. Skipped.'.format(s=snapshot, qs=', '.join(self.queue_by_name.keys())))

        # sort queues:
        for queue in self.queues:
            queue.snapshots = sorted(queue.snapshots, key=lambda s: s.time, reverse=True)

    def create_snapshot(self):
        """Returns a new snapshot of source directory."""

        name = '{queue}-{ts:%Y%m%d%H%M%S}'.format(queue=self.queues[0].name, ts=self.srcdir_time)
        _logger.info('Creating hard-linked snapshot {} of source directory.'.format(name))

        path = os.path.join(self.dstdir, name)

        try:
            shutil.copytree(self.srcdir, path, copy_function=os.link)
            _logger.debug('Hard-linked copy complete.')
            return Snapshot(path)
        except shutil.Error as e:
            _logger.error('Creation of hard-linked tree copy failed: {}'.format(e))
            _logger.debug('Trying to clean up invalid copy.')
            shutil.rmtree(path, ignore_errors=True)
            return None

    def consolidate(self):
        """Ensures queue snapshots satisfy queue specifications by deleting and moving snapshots between queues."""

        for queue in self.queues:
            queue.consolidate()
