from unittest import mock
import pytest
from history import BackupQueueSpec, FolderHistory, BackupQueueSpecError, SourceDirError, DestinationDirError


def test_backup_queue_spec():
    s = BackupQueueSpec(mock.sentinel.NAME, mock.sentinel.AGE, length=mock.sentinel.LENGTH)
    assert s.name is mock.sentinel.NAME
    assert s.age is mock.sentinel.AGE
    assert s.length is mock.sentinel.LENGTH


def test_folder_history_queue_check():
    with pytest.raises(BackupQueueSpecError):
        s1 = BackupQueueSpec(mock.sentinel.NAME1, 0, 0)
        s2 = BackupQueueSpec(mock.sentinel.NAME2, 0, 0)
        h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, [s1, s2])
        h._verify_queue_specs()

    with pytest.raises(BackupQueueSpecError):
        s1 = BackupQueueSpec(mock.sentinel.NAME1, age=0, length=0)
        s2 = BackupQueueSpec(mock.sentinel.NAME2, age=10, length=1)
        h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, [s1, s2])
        h._verify_queue_specs()

    with pytest.raises(BackupQueueSpecError):
        s1 = BackupQueueSpec(mock.sentinel.NAME1, age=10, length=2)
        s2 = BackupQueueSpec(mock.sentinel.NAME2, age=19, length=1)
        h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, [s1, s2])
        h._verify_queue_specs()

    with pytest.raises(BackupQueueSpecError):
        s1 = BackupQueueSpec(mock.sentinel.NAME1, age=10, length=2)
        s2 = BackupQueueSpec(mock.sentinel.NAME2, age=20, length=0)
        h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, [s1, s2])
        h._verify_queue_specs()

    s1 = BackupQueueSpec(mock.sentinel.NAME1, age=10, length=2)
    s2 = BackupQueueSpec(mock.sentinel.NAME2, age=20, length=1)
    h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, [s1, s2])
    h._verify_queue_specs()

    s1 = BackupQueueSpec(mock.sentinel.NAME1, age=10, length=2)
    s2 = BackupQueueSpec(mock.sentinel.NAME2, age=21, length=2)
    h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, [s1, s2])
    h._verify_queue_specs()

    s1 = BackupQueueSpec(mock.sentinel.NAME1, age=1, length=7)
    s2 = BackupQueueSpec(mock.sentinel.NAME2, age=7, length=4)
    s3 = BackupQueueSpec(mock.sentinel.NAME2, age=28, length=12)
    s4 = BackupQueueSpec(mock.sentinel.NAME2, age=365, length=10)
    h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, [s1, s2, s3, s4])
    h._verify_queue_specs()


@mock.patch('history.os')
def test_folder_history_directory_check_no_srcdir(mock_os):
    mock_os.path.exists = mock.MagicMock(side_effect=lambda dir: dir != mock.sentinel.SRCDIR)
    mock_os.path.isdir = mock.MagicMock(return_value=False)

    with pytest.raises(SourceDirError):
        h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, mock.sentinel.QUEUES)
        h._verify_directories()

    mock_os.path.exists = mock.MagicMock(return_value=True)
    mock_os.path.isdir = mock.MagicMock(side_effect=lambda dir: dir != mock.sentinel.SRCDIR)

    with pytest.raises(SourceDirError):
        h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, mock.sentinel.QUEUES)
        h._verify_directories()


@mock.patch('history.os')
def test_folder_history_directory_check_no_dstdir(mock_os):
    mock_os.path.exists = mock.MagicMock(side_effect=lambda dir: dir != mock.sentinel.DSTDIR)
    mock_os.path.isdir = mock_os.path.exists
    mock_os.makedirs = mock.MagicMock()

    h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, mock.sentinel.QUEUES)
    h._verify_directories()
    mock_os.makedirs.assert_called_once_with(mock.sentinel.DSTDIR)

    # existing but not a folder:
    mock_os.path.exists = mock.MagicMock(return_value=True)
    mock_os.makedirs.reset_mock()

    with pytest.raises(DestinationDirError):
        h = FolderHistory(mock.sentinel.SRCDIR, mock.sentinel.DSTDIR, mock.sentinel.QUEUES)
        h._verify_directories()

    assert mock_os.makedirs.call_count == 0
