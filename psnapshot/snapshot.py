"""Single snapshot."""
import datetime
import logging
import os
import re
import shutil

from psnapshot.exceptions import SnapshotDirError, SourceDirError, DestinationDirError, QueueSpecError

_logger = logging.getLogger(__name__)


class Snapshot:
    """Single snapshot of source folder."""

    SNAPSHOT_NAME_PATTERN = re.compile(r'^(?P<queue>\w+)-(?P<timestamptext>\d{14})$')
    SNAPSHOT_NAME_FORMAT = '{queue}-{time:%Y%m%d%H%M%S}'

    def __init__(self, dirpath):
        self.dirpath = dirpath
        self.name = os.path.basename(self.dirpath)

        if not os.path.exists(dirpath):
            raise SnapshotDirError('Snapshot directory {} does not exist.'.format(dirpath))

        self.queue_name, self.time = self.parse_name(self.name)

    def __str__(self):
        return self.name

    @classmethod
    def build_name(cls, queue_name, time):
        return cls.SNAPSHOT_NAME_FORMAT.format(queue=queue_name, time=time)

    @classmethod
    def parse_name(cls, name):
        m = cls.SNAPSHOT_NAME_PATTERN.match(name)
        if not m:
            raise SnapshotDirError('Snapshot directory name {} does not match naming pattern.'.format(name))

        queue_name = m.group('queue')

        timestamp_text = m.group('timestamptext')
        year = int(timestamp_text[0:4])
        month = int(timestamp_text[4:6])
        day = int(timestamp_text[6:8])
        hour = int(timestamp_text[8:10])
        minute = int(timestamp_text[10:12])
        second = int(timestamp_text[12:14])

        time = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second)

        return queue_name, time

    def move(self, queue_name):
        """Moves this snapshot to given queue by renaming the directory if needed."""
        if self.queue_name != queue_name:
            name = self.build_name(queue_name, self.time)
            dstdir = os.path.dirname(self.dirpath)
            dirpath = os.path.join(dstdir, name)

            _logger.debug('Renaming snapshot {old} to {new}.'.format(old=self.name, new=name))
            os.rename(self.dirpath, dirpath)

            self.dirpath = dirpath
            self.name = name
            self.queue_name = queue_name

    def delete(self):
        """Deletes snapshot from disk."""
        _logger.debug('Deleting snaphot {} from disk.'.format(self.name))
        shutil.rmtree(self.dirpath)
        self.dirpath = None
        self.name = None
        self.queue_name = None
        self.time = None


class Queue:
    """Ordered list of snapshots.

    :ivar name: Name of the queue.
    :ivar delta: Number of days between two snapshots in this queue.
    :ivar length: Number of snapshots in consolidated queue.
    :ivar snapshots: List of snapshots currently in queue.
    """

    QUEUE_TEXT_PATTERN = re.compile(r'^(?P<name>\w+)\[(?P<length>\d+)\]\+(?P<delta>\d+)$')

    def __init__(self, name, delta, length):
        self.name = name
        self.delta = delta if not isinstance(delta, str) else int(delta)
        self.length = length if not isinstance(length, str) else int(length)

        self.snapshots = []
        self.timedelta = datetime.timedelta(days=self.delta)

    @classmethod
    def from_textual_spec(cls, textspec):
        """Creates a queue from a textual specification like daily[7]+1."""
        if textspec:
            m = cls.QUEUE_TEXT_PATTERN.match(textspec)
            if m:
                return cls(**m.groupdict())

        raise AttributeError('textspec cannot be parsed.')

    def push_snapshots(self, snapshots):
        """Pushes new snapshots to beginning of this queue and returns the snapshots falling off the other end."""

        popped = []
        for snapshot in reversed(snapshots):
            popped[:0] = self.push_snapshot(snapshot)
        return popped

    def push_snapshot(self, snapshot):
        """Pushes a new snapshot to beginning of queue and returns the snapshots falling off the other end."""

        # snapshots are only accepted if the newest one is old enough with respect to specified delta time:
        if not self.snapshots or (snapshot.time - self.snapshots[0].time >= self.timedelta):
            _logger.info('Accepting snapshot {s} in queue {q}.'.format(s=snapshot.name, q=self.name))
            self.snapshots.insert(0, snapshot)
            snapshot.move(self.name)
        else:
            _logger.info('Snapshot {s} not accepted in queue {q}, deleting it.'.format(s=snapshot.name, q=self.name))
            snapshot.delete()

        # cleanup old snapshots:
        popped = self.snapshots[self.length:]
        if popped:
            _logger.info('Popping {n} snapshots from end of queue {q}'.format(n=len(popped), q=self.name))
            self.snapshots = self.snapshots[:self.length]

        return popped


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
                try:
                    snapshot = Snapshot(fullpath)
                    queue = self.queue_by_name.get(snapshot.queue_name)
                    if queue:
                        _logger.debug('Found snapshot {s}, part of queue {q}.'.format(s=snapshot.name, q=queue.name))
                        queue.snapshots.append(snapshot)
                    else:
                        queue_names = ', '.join(self.queue_by_name.keys())
                        _logger.warning('Snapshot {s} cannot be mapped to any of these queues: {qs}. Skipped.'.format(s=snapshot, qs=queue_names))
                except SnapshotDirError:
                    _logger.debug('Destination folder contains directory {} that does not match naming convention. Skipped.'.format(entry))

        # sort queues:
        for queue in self.queues:
            queue.snapshots = sorted(queue.snapshots, key=lambda s: s.time, reverse=True)

    def create_snapshot(self):
        """Returns a new snapshot of source directory."""

        name = Snapshot.build_name(self.queues[0].name, self.srcdir_time)
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

    def push(self, snapshot):
        """Pushes a new snapshot into first queue and propagates possible queue updates. Returns flag, whether new snapshot was added."""

        propagated_snapshots = (snapshot,)
        for queue in self.queues:
            propagated_snapshots = queue.push_snapshots(propagated_snapshots)

        # snapshots popping from last queue are no longer required:
        for snapshot in propagated_snapshots:
            snapshot.delete()
