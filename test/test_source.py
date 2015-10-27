import datetime
from unittest import mock
import pytest
from exceptions import SourceDirError
from source import Source


@mock.patch('source.os')
def test_non_existent_directory(mock_os):
    mock_os.path.exists = mock.MagicMock(return_value=False)

    with pytest.raises(SourceDirError):
        src = Source(mock.sentinel.SRCDIR)


@mock.patch('source.os')
def test_timestamp_no_files(mock_os):
    mock_os.path.exists = mock.MagicMock(return_value=True)
    mock_os.walk = mock.MagicMock(return_value=[(mock.sentinel.SRCDIR, [], [])])
    src = Source(mock.sentinel.SRCDIR)
    assert src.time is None


@mock.patch('source.os')
def test_time(mock_os):
    mock_os.path.exists = mock.MagicMock(return_value=True)
    mock_os.walk = mock.MagicMock(return_value=[(mock.sentinel.SRCDIR, [], [mock.sentinel.FILENAME1, mock.sentinel.FILENAME2])])
    mock_os.path.join = mock.MagicMock(side_effect=lambda d, filename: filename)

    mtime_by_file = {
        mock.sentinel.FILENAME1: datetime.datetime(2014, 1, 1).timestamp(),  # older
        mock.sentinel.FILENAME2: datetime.datetime(2015, 2, 3, 4, 6).timestamp()  # newer
    }
    mock_os.path.getmtime = mock.MagicMock(side_effect=lambda filename: mtime_by_file[filename])

    src = Source(mock.sentinel.SRCDIR)
    assert src.time == datetime.datetime(2015, 2, 3, 4, 6)
