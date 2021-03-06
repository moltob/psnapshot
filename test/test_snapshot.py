import datetime
import shutil
from unittest import mock

import pytest
from psnapshot.exceptions import SnapshotDirError, SourceDirError, DestinationDirError, QueueSpecError
from psnapshot.snapshot import Snapshot, Organizer, Queue


@mock.patch('psnapshot.snapshot.os')
def test_snapshot_non_existent_directory(mock_os):
    mock_os.path.exists = mock.MagicMock(return_value=False)

    with pytest.raises(SnapshotDirError):
        Snapshot('non/existing/dir')


@mock.patch('psnapshot.snapshot.os')
def test_snapshot_wrong_name_pattern(mock_os):
    mock_os.path.exists = mock.MagicMock(return_value=True)
    mock_os.path.basename = mock.MagicMock(return_value='dir')

    with pytest.raises(SnapshotDirError):
        Snapshot('non/existing/dir')


@mock.patch('psnapshot.snapshot.os')
def test_snapshot_name_pattern_parse(mock_os):
    mock_os.path.exists = mock.MagicMock(return_value=True)
    mock_os.path.basename = mock.MagicMock(return_value='queue-20150102030405')

    s = Snapshot('some/dir/queue-20150102030405')
    assert s.name == 'queue-20150102030405'
    assert s.queue_name == 'queue'
    assert s.time == datetime.datetime(2015, 1, 2, 3, 4, 5)


@mock.patch('psnapshot.snapshot.os')
def test_organizer_construction(mock_os):
    exist_by_file = {}
    mock_os.path.exists = mock.MagicMock(side_effect=lambda f: exist_by_file[f])

    exist_by_file[mock.sentinel.SRCDIR] = False
    exist_by_file[mock.sentinel.DSTDIR] = True
    with pytest.raises(SourceDirError):
        Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (mock.MagicMock(),))

    exist_by_file[mock.sentinel.SRCDIR] = True
    exist_by_file[mock.sentinel.DSTDIR] = False
    with pytest.raises(DestinationDirError):
        Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (mock.MagicMock(),))

    exist_by_file[mock.sentinel.SRCDIR] = True
    exist_by_file[mock.sentinel.DSTDIR] = True
    Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (mock.MagicMock(),))

    with pytest.raises(QueueSpecError):
        Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, [])


@mock.patch('psnapshot.snapshot.os')
def test_snapshot_move(mock_os):
    prepare_os_with_directory_list(mock_os)
    mock_os.rename = mock.MagicMock()
    s = Snapshot('queue1-20151029073630')

    s.move('queue1')
    assert not mock_os.rename.called

    s.move('queue2')
    mock_os.rename.assert_called_once_with('queue1-20151029073630', 'queue2-20151029073630')


@mock.patch('psnapshot.snapshot.os')
@mock.patch('psnapshot.snapshot.shutil')
def test_snapshot_delete(mock_shutil, mock_os):
    prepare_os_with_directory_list(mock_os)
    mock_os.rename = mock.MagicMock()
    mock_shutil.rmtree = mock.MagicMock()

    s = Snapshot('queue1-20151029073630')
    s.delete()

    mock_shutil.rmtree.assert_called_once_with('queue1-20151029073630')
    assert not s.dirpath
    assert not s.name
    assert not s.time
    assert not s.queue_name


def test_queue_from_textual_spec():
    q = Queue.from_textual_spec('daily[7]+1')
    assert q.name == 'daily'
    assert q.length == 7
    assert q.delta == 1

    q = Queue.from_textual_spec('weekly[4]+7')
    assert q.name == 'weekly'
    assert q.length == 4
    assert q.delta == 7

    with pytest.raises(AttributeError):
        Queue.from_textual_spec('some[z]-3')

    with pytest.raises(AttributeError):
        Queue.from_textual_spec(None)


def test_queue_push_snapshot_first():
    mock_snapshot = mock.MagicMock()

    q = Queue(mock.sentinel.NAME, 1, 1)
    popped = q.push_snapshot(mock_snapshot)

    assert q.snapshots == [mock_snapshot]
    mock_snapshot.move.assert_called_once_with(mock.sentinel.NAME)
    assert not mock_snapshot.delete.called
    assert not popped


def test_queue_push_snapshot_too_recent():
    mock_snapshot1 = mock.MagicMock()
    mock_snapshot1.time = datetime.datetime(2015, 10, 29, 20, 35)
    mock_snapshot2 = mock.MagicMock()
    mock_snapshot2.time = datetime.datetime(2015, 10, 29, 21, 35)  # not yet one day apart

    q = Queue(mock.sentinel.NAME, 1, 1)
    q.snapshots = [mock_snapshot1]

    popped = q.push_snapshot(mock_snapshot2)

    assert q.snapshots == [mock_snapshot1]
    assert not mock_snapshot1.delete.called
    assert mock_snapshot2.delete.called
    assert not popped


def test_queue_push_snapshot_accepted():
    mock_snapshot1 = mock.MagicMock()
    mock_snapshot1.time = datetime.datetime(2015, 10, 25, 20, 35)
    mock_snapshot2 = mock.MagicMock()
    mock_snapshot2.time = datetime.datetime(2015, 10, 29, 21, 35)

    q = Queue(mock.sentinel.NAME, delta=2, length=5)
    q.snapshots = [mock_snapshot1]

    popped = q.push_snapshot(mock_snapshot2)

    assert q.snapshots == [mock_snapshot2, mock_snapshot1]
    assert not mock_snapshot1.delete.called
    assert not mock_snapshot2.delete.called
    assert not popped


def test_queue_push_snapshot_accepted_rounded():
    mock_snapshot1 = mock.MagicMock()
    mock_snapshot1.time = datetime.datetime(2015, 10, 25, 20, 35)
    mock_snapshot2 = mock.MagicMock()
    mock_snapshot2.time = datetime.datetime(2015, 10, 27, 20, 30)

    q = Queue(mock.sentinel.NAME, delta=2, length=5)
    q.snapshots = [mock_snapshot1]

    popped = q.push_snapshot(mock_snapshot2)

    assert q.snapshots == [mock_snapshot2, mock_snapshot1]
    assert not mock_snapshot1.delete.called
    assert not mock_snapshot2.delete.called
    assert not popped


def test_queue_push_snapshot_length_exceeded():
    mock_snapshot1 = mock.MagicMock()
    mock_snapshot1.time = datetime.datetime(2015, 10, 20, 20, 35)
    mock_snapshot2 = mock.MagicMock()
    mock_snapshot2.time = datetime.datetime(2015, 10, 23, 21, 35)
    mock_snapshot3 = mock.MagicMock()
    mock_snapshot3.time = datetime.datetime(2015, 10, 26, 21, 35)

    q = Queue(mock.sentinel.NAME, delta=2, length=2)
    q.snapshots = [mock_snapshot2, mock_snapshot1]

    popped = q.push_snapshot(mock_snapshot3)

    assert q.snapshots == [mock_snapshot3, mock_snapshot2]
    assert not mock_snapshot1.delete.called
    assert not mock_snapshot2.delete.called
    assert not mock_snapshot3.delete.called
    assert popped == [mock_snapshot1]


def test_queue_push_snapshots():
    mock_snapshot1 = mock.MagicMock()
    mock_snapshot1.time = datetime.datetime(2015, 10, 20, 20, 35)
    mock_snapshot2 = mock.MagicMock()
    mock_snapshot2.time = datetime.datetime(2015, 10, 23, 21, 35)
    mock_snapshot3 = mock.MagicMock()
    mock_snapshot3.time = datetime.datetime(2015, 10, 26, 21, 35)

    q = Queue(mock.sentinel.NAME, delta=2, length=1)
    q.snapshots = [mock_snapshot1]

    popped = q.push_snapshots([mock_snapshot3, mock_snapshot2])

    assert q.snapshots == [mock_snapshot3]
    assert not mock_snapshot1.delete.called
    assert not mock_snapshot2.delete.called
    assert not mock_snapshot3.delete.called
    assert popped == [mock_snapshot2, mock_snapshot1]


def prepare_os_with_directory_list(mock_os, *files):
    """Helper to set up os mock with a number of files being returned by listdir."""
    mock_os.listdir = mock.MagicMock(return_value=files)
    mock_os.link = mock.sentinel.OS_LINK
    mock_os.path.basename = mock.MagicMock(side_effect=lambda p: p)
    mock_os.path.exists = mock.MagicMock(return_value=True)
    mock_os.path.isdir = mock.MagicMock(return_value=True)
    mock_os.path.join = mock.MagicMock(side_effect=lambda *args: args[-1])


@mock.patch('psnapshot.snapshot.os')
def test_organizer_find_snapshots_none(mock_os):
    prepare_os_with_directory_list(mock_os)

    queue = Queue(mock.sentinel.QUEUE_NAME, 1, mock.sentinel.QUEUE_LENGTH)

    organizer = Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (queue,))
    organizer.find_snapshots()

    assert len(queue.snapshots) == 0


@mock.patch('psnapshot.snapshot.os')
def test_organizer_find_snapshots_wrong_name(mock_os):
    prepare_os_with_directory_list(mock_os, 'not-matching-name')

    queue = Queue(mock.sentinel.QUEUE_NAME, 1, mock.sentinel.QUEUE_LENGTH)

    organizer = Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (queue,))
    organizer.find_snapshots()

    assert len(queue.snapshots) == 0
    mock_os.path.isdir.assert_called_once_with('not-matching-name')


@mock.patch('psnapshot.snapshot.os')
def test_organizer_find_snapshots_unknown_queue(mock_os):
    prepare_os_with_directory_list(mock_os, 'queueX-20150201100907')

    queue = Queue('queue1', 1, mock.sentinel.QUEUE_LENGTH)

    organizer = Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (queue,))
    organizer.find_snapshots()

    assert len(queue.snapshots) == 0


@mock.patch('psnapshot.snapshot.os')
def test_organizer_find_snapshots_ordered(mock_os):
    prepare_os_with_directory_list(mock_os, 'queue1-20150201100907', 'not-matching-name', 'queue1-20150201100908', 'queue1-20150201100906')

    queue = Queue('queue1', 1, mock.sentinel.QUEUE_LENGTH)

    organizer = Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (queue,))
    organizer.find_snapshots()

    # backups must be ordered from newest to oldest:
    assert len(queue.snapshots) == 3
    assert queue.snapshots[0].name == 'queue1-20150201100908'
    assert queue.snapshots[1].name == 'queue1-20150201100907'
    assert queue.snapshots[2].name == 'queue1-20150201100906'


@mock.patch('psnapshot.snapshot.os')
def test_organizer_find_snapshots_multiple_queues(mock_os):
    prepare_os_with_directory_list(mock_os, 'queue1-20150201100907', 'queue2-20150201100906', 'queueX-20150201100908')

    queue1 = Queue('queue1', 1, mock.sentinel.QUEUE_LENGTH)
    queue2 = Queue('queue2', 1, mock.sentinel.QUEUE_LENGTH)

    organizer = Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (queue1, queue2))
    organizer.find_snapshots()

    assert len(queue1.snapshots) == 1
    assert queue1.snapshots[0].name == 'queue1-20150201100907'
    assert len(queue2.snapshots) == 1
    assert queue2.snapshots[0].name == 'queue2-20150201100906'


@mock.patch('psnapshot.snapshot.shutil')
@mock.patch('psnapshot.snapshot.os')
def test_link_source_ok(mock_os, mock_shutil):
    mock_shutil.copytree = mock.MagicMock(return_value=mock.sentinel.NEW_DESTINATION_PATH)
    mock_shutil.Error = shutil.Error
    prepare_os_with_directory_list(mock_os)
    mock_os.path.getmtime = mock.MagicMock(return_value=datetime.datetime(2015, 1, 1).timestamp())

    queue1 = Queue('queue1', 1, mock.sentinel.QUEUE_LENGTH)
    queue2 = Queue('queue2', 1, mock.sentinel.QUEUE_LENGTH)

    organizer = Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (queue1, queue2))
    snapshot = organizer.create_snapshot()

    mock_shutil.copytree.assert_called_once_with(mock.sentinel.SRCDIR, mock.ANY, copy_function=mock.sentinel.OS_LINK)
    assert snapshot
    assert snapshot.name == 'queue1-20150101000000'


@mock.patch('psnapshot.snapshot.shutil')
@mock.patch('psnapshot.snapshot.os')
def test_link_source_error(mock_os, mock_shutil):
    mock_shutil.copytree = mock.MagicMock(side_effect=shutil.Error)
    mock_shutil.Error = shutil.Error
    mock_shutil.rmtree = mock.MagicMock()
    prepare_os_with_directory_list(mock_os)
    mock_os.path.getmtime = mock.MagicMock(return_value=datetime.datetime(2015, 1, 1).timestamp())

    queue1 = Queue('queue1', 1, mock.sentinel.QUEUE_LENGTH)
    queue2 = Queue('queue2', 1, mock.sentinel.QUEUE_LENGTH)

    organizer = Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (queue1, queue2))
    snapshot = organizer.create_snapshot()

    mock_shutil.copytree.assert_called_once_with(mock.sentinel.SRCDIR, mock.ANY, copy_function=mock.sentinel.OS_LINK)
    mock_shutil.rmtree.assert_called_once_with('queue1-20150101000000', ignore_errors=mock.ANY)
    assert not snapshot


@mock.patch('psnapshot.snapshot.os')
def test_organizer_srcdir_time_empty_dir(mock_os):
    prepare_os_with_directory_list(mock_os)
    mock_os.path.getmtime = mock.MagicMock(return_value=datetime.datetime(2015, 1, 2, 3, 4, 5, 6).timestamp())
    mock_os.walk = mock.MagicMock(return_value=[])

    queue1 = Queue('queue1', 1, mock.sentinel.QUEUE_LENGTH)
    queue2 = Queue('queue2', 1, mock.sentinel.QUEUE_LENGTH)

    organizer = Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (queue1, queue2))

    assert organizer.srcdir_time == datetime.datetime(2015, 1, 2, 3, 4, 5)
    mock_os.path.getmtime.assert_called_once_with(mock.sentinel.SRCDIR)


@mock.patch('psnapshot.snapshot.os')
def test_organizer_srcdir_time(mock_os):
    prepare_os_with_directory_list(mock_os)
    mock_os.walk = mock.MagicMock(return_value=[(mock.sentinel.SRCDIR, [], (mock.sentinel.FILE1, mock.sentinel.FILE2))])

    time = {
        mock.sentinel.SRCDIR: datetime.datetime(2015, 1, 2),
        mock.sentinel.FILE1: datetime.datetime(2015, 2, 3),
        mock.sentinel.FILE2: datetime.datetime(2015, 3, 4, 10, 20, 30, 40),
    }
    mock_os.path.getmtime = mock.MagicMock(side_effect=lambda f: time[f].timestamp())

    queue1 = Queue('queue1', 1, mock.sentinel.QUEUE_LENGTH)
    queue2 = Queue('queue2', 1, mock.sentinel.QUEUE_LENGTH)

    organizer = Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (queue1, queue2))

    assert organizer.srcdir_time == datetime.datetime(2015, 3, 4, 10, 20, 30)


@mock.patch('psnapshot.snapshot.os')
def test_organizer_snapshots_time(mock_os):
    prepare_os_with_directory_list(mock_os)

    queue1 = Queue('queue1', 1, mock.sentinel.QUEUE_LENGTH)
    queue2 = Queue('queue2', 1, mock.sentinel.QUEUE_LENGTH)

    organizer = Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (queue1, queue2))
    assert organizer.snapshots_time is None

    queue2.snapshots = [Snapshot('queue2-20150201100906')]
    assert organizer.snapshots_time is None

    queue1.snapshots = [Snapshot('queue1-20150101100906')]
    assert organizer.snapshots_time == datetime.datetime(2015, 1, 1, 10, 9, 6)


@mock.patch('psnapshot.snapshot.os')
def test_organizer_push_snapshot(mock_os):
    prepare_os_with_directory_list(mock_os)

    mock_queue1 = mock.MagicMock()
    mock_queue2 = mock.MagicMock()

    organizer = Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (mock_queue1, mock_queue2))

    mock_snapshot_2a = mock.MagicMock()
    mock_snapshot_2b = mock.MagicMock()

    mock_queue1.push_snapshots = mock.MagicMock(return_value=(mock.sentinel.SNAPSHOT_1A, mock.sentinel.SNAPSHOT_1B))
    mock_queue2.push_snapshots = mock.MagicMock(return_value=(mock_snapshot_2a, mock_snapshot_2b))

    organizer.push(mock.sentinel.SNAPSHOT)

    mock_queue1.push_snapshots.assert_called_once_with((mock.sentinel.SNAPSHOT,))
    mock_queue2.push_snapshots.assert_called_once_with((mock.sentinel.SNAPSHOT_1A, mock.sentinel.SNAPSHOT_1B))
    assert mock_snapshot_2a.delete.called
    assert mock_snapshot_2b.delete.called

# TODO: implement top-level control and test
