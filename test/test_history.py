from unittest import mock
import pytest
from history import BackupQueueSpec, FolderHistory, BackupQueueSpecError, SourceDirError, DestinationDirError, Backup


def test_backup_queue_spec():
    s = BackupQueueSpec(mock.sentinel.NAME, mock.sentinel.AGE, length=mock.sentinel.LENGTH)
    assert s.name is mock.sentinel.NAME
    assert s.age is mock.sentinel.AGE
    assert s.length is mock.sentinel.LENGTH


def test_folder_history_queue_check():
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
def test_folder_history_directory_prep_no_srcdir(mock_os):
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
def test_folder_history_directory_prep_no_dstdir(mock_os):
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
def test_folder_history_find_backups_none(mock_os):
    mock_os.listdir = mock.MagicMock(return_value=())
    h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, mock.sentinel.QUEUES)
    h._find_backups()
    assert h.backups == []


@mock.patch('history.os')
def test_folder_history_find_backups_no_dirs(mock_os):
    mock_os.listdir = mock.MagicMock(return_value=('some-20151013070800', 'some-20151013070801'))
    mock_os.path.isdir = mock.MagicMock(return_value=False)
    h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, mock.sentinel.QUEUES)
    h._find_backups()
    assert h.backups == []


@mock.patch('history.os')
def test_folder_history_find_backups_wrong_names(mock_os):
    mock_os.listdir = mock.MagicMock(return_value=('some-2015101307080', '-20151013070801'))
    mock_os.path.isdir = mock.MagicMock(return_value=True)
    h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, mock.sentinel.QUEUES)
    h._find_backups()
    assert h.backups == []


@mock.patch('history.os')
def test_folder_history_find_backups(mock_os):
    mock_os.listdir = mock.MagicMock(return_value=('some-20151013070800', 'other-20151013070801', 'third_one-20150101000000'))
    mock_os.path.isdir = mock.MagicMock(return_value=True)
    h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, mock.sentinel.QUEUES)
    h._find_backups()

    # backups must be ordered from newest to oldest:
    assert len(h.backups) == 3
    assert h.backups[0] == Backup('other-20151013070801', 'other', '20151013070801')
    assert h.backups[1] == Backup('some-20151013070800', 'some', '20151013070800')
    assert h.backups[2] == Backup('third_one-20150101000000', 'third_one', '20150101000000')
