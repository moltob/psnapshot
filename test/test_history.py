from unittest import mock
import datetime
import pytest
from history import BackupQueueSpec, FolderHistory, BackupQueueSpecError, SourceDirError, DestinationDirError, Backup, BackupTime


def test_backup_queue_spec():
    s = BackupQueueSpec(mock.sentinel.NAME, mock.sentinel.AGE, length=mock.sentinel.LENGTH)
    assert s.name is mock.sentinel.NAME
    assert s.age is mock.sentinel.AGE
    assert s.length is mock.sentinel.LENGTH


def test_queue_check():
    with pytest.raises(BackupQueueSpecError):
        s1 = BackupQueueSpec('name1', 0, 0)
        s2 = BackupQueueSpec('name2', 0, 0)
        h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, [s1, s2])
        h._verify_queue_specs()

    with pytest.raises(BackupQueueSpecError):
        s1 = BackupQueueSpec('name1', age=0, length=0)
        s2 = BackupQueueSpec('name2', age=10, length=1)
        h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, [s1, s2])
        h._verify_queue_specs()

    with pytest.raises(BackupQueueSpecError):
        s1 = BackupQueueSpec('name1', age=10, length=2)
        s2 = BackupQueueSpec('name2', age=19, length=1)
        h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, [s1, s2])
        h._verify_queue_specs()

    with pytest.raises(BackupQueueSpecError):
        s1 = BackupQueueSpec('name1', age=10, length=2)
        s2 = BackupQueueSpec('name2', age=20, length=0)
        h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, [s1, s2])
        h._verify_queue_specs()

    with pytest.raises(BackupQueueSpecError):
        s1 = BackupQueueSpec('name-1', age=10, length=2)
        s2 = BackupQueueSpec('name-2', age=20, length=1)
        h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, [s1, s2])
        h._verify_queue_specs()

    s1 = BackupQueueSpec('name1', age=10, length=2)
    s2 = BackupQueueSpec('name2', age=20, length=1)
    h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, [s1, s2])
    h._verify_queue_specs()

    s1 = BackupQueueSpec('name1', age=10, length=2)
    s2 = BackupQueueSpec('name2', age=21, length=2)
    h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, [s1, s2])
    h._verify_queue_specs()

    s1 = BackupQueueSpec('name1', age=1, length=7)
    s2 = BackupQueueSpec('name2', age=7, length=4)
    s3 = BackupQueueSpec('name2', age=28, length=12)
    s4 = BackupQueueSpec('name2', age=365, length=10)
    h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, [s1, s2, s3, s4])
    h._verify_queue_specs()


@mock.patch('history.os')
def test_directory_prep_no_srcdir(mock_os):
    mock_os.path.exists = mock.MagicMock(side_effect=lambda dir: dir != mock.sentinel.SRCDIR)
    mock_os.path.isdir = mock.MagicMock(return_value=False)

    with pytest.raises(SourceDirError):
        h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, mock.sentinel.QUEUES)
        h._prepare_directories()

    mock_os.path.exists = mock.MagicMock(return_value=True)
    mock_os.path.isdir = mock.MagicMock(side_effect=lambda dir: dir != mock.sentinel.SRCDIR)

    with pytest.raises(SourceDirError):
        h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, mock.sentinel.QUEUES)
        h._prepare_directories()


@mock.patch('history.os')
def test_directory_prep_no_dstdir(mock_os):
    mock_os.path.exists = mock.MagicMock(side_effect=lambda dir: dir != mock.sentinel.DSTDIR)
    mock_os.path.isdir = mock_os.path.exists
    mock_os.makedirs = mock.MagicMock()

    h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, mock.sentinel.QUEUES)
    h._prepare_directories()
    mock_os.makedirs.assert_called_once_with(mock.sentinel.DSTDIR)

    # existing but not a folder:
    mock_os.path.exists = mock.MagicMock(return_value=True)
    mock_os.makedirs.reset_mock()

    with pytest.raises(DestinationDirError):
        h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, mock.sentinel.QUEUES)
        h._prepare_directories()

    assert mock_os.makedirs.call_count == 0


@mock.patch('history.os')
def test_find_backups_none(mock_os):
    mock_os.listdir = mock.MagicMock(return_value=())
    h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, mock.sentinel.QUEUES)
    h._find_backups()
    assert h.backups == []


@mock.patch('history.os')
def test_find_backups_no_dirs(mock_os):
    mock_os.listdir = mock.MagicMock(return_value=('some-20151013070800', 'some-20151013070801'))
    mock_os.path.isdir = mock.MagicMock(return_value=False)
    h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, mock.sentinel.QUEUES)
    h._find_backups()
    assert h.backups == []


@mock.patch('history.os')
def test_find_backups_wrong_names(mock_os):
    mock_os.listdir = mock.MagicMock(return_value=('some-2015101307080', '-20151013070801'))
    mock_os.path.isdir = mock.MagicMock(return_value=True)
    h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, mock.sentinel.QUEUES)
    h._find_backups()
    assert h.backups == []


@mock.patch('history.os')
def test_find_backups(mock_os):
    mock_os.listdir = mock.MagicMock(return_value=('some-20151013070800', 'other-20151013070801', 'third_one-20150101000000'))
    mock_os.path.isdir = mock.MagicMock(return_value=True)
    h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, mock.sentinel.QUEUES)
    h._find_backups()

    # backups must be ordered from newest to oldest:
    assert len(h.backups) == 3
    assert h.backups[0] == Backup('other-20151013070801', 'other', BackupTime.fromtext('20151013070801'))
    assert h.backups[1] == Backup('some-20151013070800', 'some', BackupTime.fromtext('20151013070800'))
    assert h.backups[2] == Backup('third_one-20150101000000', 'third_one', BackupTime.fromtext('20150101000000'))


def test_get_backup_time():
    h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, mock.sentinel.QUEUES)
    h.backups = [Backup(mock.sentinel.NAME, mock.sentinel.QUEUE, BackupTime.fromtext('19720217155215'))]
    assert h.backup_timestamp == datetime.datetime(1972, 2, 17, 15, 52, 15)


@mock.patch('history.os')
def test_source_dir_modified_second_file_newer(mock_os):
    h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, mock.sentinel.QUEUES)
    h.backups = [Backup(mock.sentinel.NAME, mock.sentinel.QUEUE, BackupTime.fromtext('20150203040506'))]

    mock_os.walk = mock.MagicMock(return_value=[(h.srcdir, [], [mock.sentinel.FILENAME1, mock.sentinel.FILENAME2])])
    mock_os.path.join = mock.MagicMock(side_effect=lambda d, filename: filename)

    mtime_by_file = {
        mock.sentinel.FILENAME1: datetime.datetime(2014, 1, 1).timestamp(),  # older
        mock.sentinel.FILENAME2: datetime.datetime(2015, 2, 3, 4, 6).timestamp()  # newer
    }
    mock_os.path.getmtime = mock.MagicMock(side_effect=lambda filename: mtime_by_file[filename])

    assert h.srcdir_timestamp
    assert h.srcdir_updated
    assert mock_os.path.getmtime.call_count == 2


@mock.patch('history.os')
def test_source_dir_modified_files_older(mock_os):
    h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, mock.sentinel.QUEUES)
    h.backups = [Backup(mock.sentinel.NAME, mock.sentinel.QUEUE, BackupTime.fromtext('20150203040517'))]

    mock_os.walk = mock.MagicMock(return_value=[(h.srcdir, [], [mock.sentinel.FILENAME1, mock.sentinel.FILENAME2])])
    mock_os.path.join = mock.MagicMock(side_effect=lambda d, filename: filename)

    mtime_by_file = {
        mock.sentinel.FILENAME1: datetime.datetime(2014, 1, 1).timestamp(),
        mock.sentinel.FILENAME2: datetime.datetime(2015, 2, 3, 4, 4).timestamp()
    }
    mock_os.path.getmtime = mock.MagicMock(side_effect=lambda filename: mtime_by_file[filename])

    assert not h.srcdir_updated
    assert mock_os.path.getmtime.call_count == 2


@mock.patch('history.shutil')
@mock.patch('history.os')
def test_link_source_error(mock_os, mock_shutil):
    class MockException(Exception):
        pass

    mock_shutil.Error = MockException
    mock_shutil.copytree = mock.MagicMock(side_effect=MockException)
    mock_os.path.join = mock.MagicMock(side_effect=lambda d, filename: filename)
    mock_os.link = mock.sentinel.OS_LINK

    s1 = BackupQueueSpec('name1', age=10, length=2)
    s2 = BackupQueueSpec('name2', age=20, length=1)
    h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, [s1, s2])
    h._srcdir_timestamp = datetime.datetime(2015, 1, 1)
    h.backups = [Backup(mock.sentinel.NAME, mock.sentinel.QUEUE, BackupTime.fromtext('20150203040517'))]
    backuptime = h.backup_timestamp

    h._link_source()

    mock_shutil.copytree.assert_called_once_with(mock.sentinel.SRCDIR, mock.ANY, copy_function=mock.sentinel.OS_LINK)
    assert h.backup_timestamp is backuptime
    assert len(h.backups) == 1


@mock.patch('history.shutil')
@mock.patch('history.os')
def test_link_source_ok(mock_os, mock_shutil):
    mock_shutil.copytree = mock.MagicMock(return_value=mock.sentinel.NEW_DESTINATION_PATH)
    mock_os.path.join = mock.MagicMock(side_effect=lambda d, filename: filename)
    mock_os.link = mock.sentinel.OS_LINK

    s1 = BackupQueueSpec('name1', age=10, length=2)
    s2 = BackupQueueSpec('name2', age=20, length=1)
    h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, [s1, s2])
    h._srcdir_timestamp = datetime.datetime(2015, 1, 1)
    h.backups = [Backup(mock.sentinel.NAME, mock.sentinel.QUEUE, BackupTime.fromtext('20150203040517'))]

    h._link_source()

    mock_shutil.copytree.assert_called_once_with(mock.sentinel.SRCDIR, mock.ANY, copy_function=mock.sentinel.OS_LINK)
    assert h.backup_timestamp is h.srcdir_timestamp
    assert len(h.backups) == 2
    assert h.backups[0].queue == 'name1'


@mock.patch('history.os')
def test_timestamps_rounded_days(mock_os):
    h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, mock.sentinel.QUEUES)
    h.backups = [Backup(mock.sentinel.NAME, mock.sentinel.QUEUE, BackupTime.fromtext('20150203040506'))]

    mock_os.walk = mock.MagicMock(return_value=[(h.srcdir, [], [mock.sentinel.FILENAME])])
    mock_os.path.join = mock.MagicMock(side_effect=lambda d, filename: filename)

    mtime_by_file = {
        mock.sentinel.FILENAME: datetime.datetime(2015, 2, 3, 4, 5, 6, microsecond=10).timestamp()
    }
    mock_os.path.getmtime = mock.MagicMock(side_effect=lambda filename: mtime_by_file[filename])

    assert h.srcdir_timestamp
    assert not h.srcdir_updated


def test_backup_time_text():
    t = BackupTime(datetime.datetime(2015, 1, 2, 3, 4, 5))
    assert t.text == '20150102030405'
    assert str(t) == t.text


def test_backup_time_fromtext():
    t = BackupTime.fromtext('20150910111213')
    assert t.stamp == datetime.datetime(2015, 9, 10, 11, 12, 13)
    assert t.text == '20150910111213'
