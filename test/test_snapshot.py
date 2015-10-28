import datetime
from unittest import mock
import pytest
import shutil
from exceptions import SnapshotDirError, SourceDirError, DestinationDirError, QueueSpecError
from snapshot import Snapshot, Organizer, Queue


@mock.patch('snapshot.os')
def test_snapshot_non_existent_directory(mock_os):
    mock_os.path.exists = mock.MagicMock(return_value=False)

    with pytest.raises(SnapshotDirError):
        Snapshot('non/existing/dir')


@mock.patch('snapshot.os')
def test_snapshot_wrong_name_pattern(mock_os):
    mock_os.path.exists = mock.MagicMock(return_value=True)
    mock_os.path.basename = mock.MagicMock(return_value='dir')

    with pytest.raises(SnapshotDirError):
        Snapshot('non/existing/dir')


@mock.patch('snapshot.os')
def test_snapshot_name_pattern_parse(mock_os):
    mock_os.path.exists = mock.MagicMock(return_value=True)
    mock_os.path.basename = mock.MagicMock(return_value='queue-20150102030405')

    s = Snapshot('some/dir/queue-20150102030405')
    assert s.name == 'queue-20150102030405'
    assert s.queue_name == 'queue'
    assert s.time == datetime.datetime(2015, 1, 2, 3, 4, 5)


@mock.patch('snapshot.os')
def test_organizer_construction(mock_os):
    exist_by_file = {}
    mock_os.path.exists = mock.MagicMock(side_effect=lambda f: exist_by_file[f])

    exist_by_file[mock.sentinel.SRCDIR] = False
    exist_by_file[mock.sentinel.DSTDIR] = True
    with pytest.raises(SourceDirError):
        Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (mock.sentinel.QUEUE,))

    exist_by_file[mock.sentinel.SRCDIR] = True
    exist_by_file[mock.sentinel.DSTDIR] = False
    with pytest.raises(DestinationDirError):
        Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (mock.sentinel.QUEUE,))

    exist_by_file[mock.sentinel.SRCDIR] = True
    exist_by_file[mock.sentinel.DSTDIR] = True
    Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (mock.sentinel.QUEUE,))

    with pytest.raises(QueueSpecError):
        Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, [])


def prepare_os_with_directory_list(mock_os, *files):
    """Helper to set up os mock with a number of files being returned by listdir."""
    mock_os.listdir = mock.MagicMock(return_value=files)
    mock_os.link = mock.sentinel.OS_LINK
    mock_os.path.basename = mock.MagicMock(side_effect=lambda p: p)
    mock_os.path.exists = mock.MagicMock(return_value=True)
    mock_os.path.isdir = mock.MagicMock(return_value=True)
    mock_os.path.join = mock.MagicMock(side_effect=lambda *args: args[-1])


@mock.patch('snapshot.os')
def test_organizer_find_snapshots_none(mock_os):
    prepare_os_with_directory_list(mock_os)

    queue = Queue(mock.sentinel.QUEUE_NAME, mock.sentinel.QUEUE_DELTA, mock.sentinel.QUEUE_LENGTH)

    organizer = Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (queue,))
    organizer.find_snapshots()

    assert len(queue.snapshots) == 0


@mock.patch('snapshot.os')
def test_organizer_find_snapshots_wrong_name(mock_os):
    prepare_os_with_directory_list(mock_os, 'not-matching-name')

    queue = Queue(mock.sentinel.QUEUE_NAME, mock.sentinel.QUEUE_DELTA, mock.sentinel.QUEUE_LENGTH)

    organizer = Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (queue,))
    organizer.find_snapshots()

    assert len(queue.snapshots) == 0
    mock_os.path.isdir.assert_called_once_with('not-matching-name')


@mock.patch('snapshot.os')
def test_organizer_find_snapshots_unknown_queue(mock_os):
    prepare_os_with_directory_list(mock_os, 'queueX-20150201100907')

    queue = Queue('queue1', mock.sentinel.QUEUE_DELTA, mock.sentinel.QUEUE_LENGTH)

    organizer = Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (queue,))
    organizer.find_snapshots()

    assert len(queue.snapshots) == 0


@mock.patch('snapshot.os')
def test_organizer_find_snapshots_ordered(mock_os):
    prepare_os_with_directory_list(mock_os, 'queue1-20150201100907', 'not-matching-name', 'queue1-20150201100908', 'queue1-20150201100906')

    queue = Queue('queue1', mock.sentinel.QUEUE_DELTA, mock.sentinel.QUEUE_LENGTH)

    organizer = Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (queue,))
    organizer.find_snapshots()

    # backups must be ordered from newest to oldest:
    assert len(queue.snapshots) == 3
    assert queue.snapshots[0].name == 'queue1-20150201100908'
    assert queue.snapshots[1].name == 'queue1-20150201100907'
    assert queue.snapshots[2].name == 'queue1-20150201100906'


@mock.patch('snapshot.os')
def test_organizer_find_snapshots_multiple_queues(mock_os):
    prepare_os_with_directory_list(mock_os, 'queue1-20150201100907', 'queue2-20150201100906', 'queueX-20150201100908')

    queue1 = Queue('queue1', mock.sentinel.QUEUE_DELTA, mock.sentinel.QUEUE_LENGTH)
    queue2 = Queue('queue2', mock.sentinel.QUEUE_DELTA, mock.sentinel.QUEUE_LENGTH)

    organizer = Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (queue1, queue2))
    organizer.find_snapshots()

    assert len(queue1.snapshots) == 1
    assert queue1.snapshots[0].name == 'queue1-20150201100907'
    assert len(queue2.snapshots) == 1
    assert queue2.snapshots[0].name == 'queue2-20150201100906'


@mock.patch('snapshot.shutil')
@mock.patch('snapshot.os')
def test_link_source_ok(mock_os, mock_shutil):
    mock_shutil.copytree = mock.MagicMock(return_value=mock.sentinel.NEW_DESTINATION_PATH)
    mock_shutil.Error = shutil.Error
    prepare_os_with_directory_list(mock_os)
    mock_os.path.getmtime = mock.MagicMock(return_value=datetime.datetime(2015, 1, 1).timestamp())

    queue1 = Queue('queue1', mock.sentinel.QUEUE_DELTA, mock.sentinel.QUEUE_LENGTH)
    queue2 = Queue('queue2', mock.sentinel.QUEUE_DELTA, mock.sentinel.QUEUE_LENGTH)
    queue1.snapshots = []
    queue2.snapshots = []

    organizer = Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (queue1, queue2))
    organizer.create_snapshot()

    mock_shutil.copytree.assert_called_once_with(mock.sentinel.SRCDIR, mock.ANY, copy_function=mock.sentinel.OS_LINK)
    assert len(queue1.snapshots) == 1
    assert queue1.snapshots[0].name == 'queue1-20150101000000'
    assert not queue2.snapshots


@mock.patch('snapshot.shutil')
@mock.patch('snapshot.os')
def test_link_source_error(mock_os, mock_shutil):
    mock_shutil.copytree = mock.MagicMock(side_effect=shutil.Error)
    mock_shutil.Error = shutil.Error
    mock_shutil.rmtree = mock.MagicMock()
    prepare_os_with_directory_list(mock_os)
    mock_os.path.getmtime = mock.MagicMock(return_value=datetime.datetime(2015, 1, 1).timestamp())

    queue1 = Queue('queue1', mock.sentinel.QUEUE_DELTA, mock.sentinel.QUEUE_LENGTH)
    queue2 = Queue('queue2', mock.sentinel.QUEUE_DELTA, mock.sentinel.QUEUE_LENGTH)
    queue1.snapshots = []
    queue2.snapshots = []

    organizer = Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (queue1, queue2))
    organizer.create_snapshot()

    mock_shutil.copytree.assert_called_once_with(mock.sentinel.SRCDIR, mock.ANY, copy_function=mock.sentinel.OS_LINK)
    mock_shutil.rmtree.assert_called_once_with('queue1-20150101000000', ignore_errors=mock.ANY)
    assert not queue1.snapshots
    assert not queue2.snapshots


@mock.patch('snapshot.os')
def test_organizer_srcdir_time_empty_dir(mock_os):
    prepare_os_with_directory_list(mock_os)
    mock_os.path.getmtime = mock.MagicMock(return_value=datetime.datetime(2015, 1, 2, 3, 4, 5, 6).timestamp())
    mock_os.walk = mock.MagicMock(return_value=[])

    queue1 = Queue('queue1', mock.sentinel.QUEUE_DELTA, mock.sentinel.QUEUE_LENGTH)
    queue2 = Queue('queue2', mock.sentinel.QUEUE_DELTA, mock.sentinel.QUEUE_LENGTH)

    organizer = Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (queue1, queue2))

    assert organizer.srcdir_time == datetime.datetime(2015, 1, 2, 3, 4, 5)
    mock_os.path.getmtime.assert_called_once_with(mock.sentinel.SRCDIR)


@mock.patch('snapshot.os')
def test_organizer_srcdir_time(mock_os):
    prepare_os_with_directory_list(mock_os)
    mock_os.walk = mock.MagicMock(return_value=[(mock.sentinel.SRCDIR, [], (mock.sentinel.FILE1, mock.sentinel.FILE2))])

    time = {
        mock.sentinel.SRCDIR: datetime.datetime(2015, 1, 2),
        mock.sentinel.FILE1: datetime.datetime(2015, 2, 3),
        mock.sentinel.FILE2: datetime.datetime(2015, 3, 4, 10, 20, 30, 40),
    }
    mock_os.path.getmtime = mock.MagicMock(side_effect=lambda f: time[f].timestamp())

    queue1 = Queue('queue1', mock.sentinel.QUEUE_DELTA, mock.sentinel.QUEUE_LENGTH)
    queue2 = Queue('queue2', mock.sentinel.QUEUE_DELTA, mock.sentinel.QUEUE_LENGTH)

    organizer = Organizer(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, (queue1, queue2))

    assert organizer.srcdir_time == datetime.datetime(2015, 3, 4, 10, 20, 30)

# TODO: implement snapshot consolidation and test
# TODO: implement top-level control and test
