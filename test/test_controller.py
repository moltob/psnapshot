"""Back to back tests."""
import datetime
import os
import shutil

from control import SnapshotController
from snapshot import Queue

SRCDIR = os.path.join(os.path.dirname(__file__), 'resources', 'testsrcdir')
DSTDIR = os.path.join(os.path.dirname(__file__), 'resources', 'testdstdir')


def prepare_srcdir(dirtime, filetime_a, filetime_b):
    if os.path.exists(SRCDIR):
        shutil.rmtree(SRCDIR)
    os.makedirs(SRCDIR)

    os.mkdir(os.path.join(SRCDIR, 'subdir'))

    fname = os.path.join(SRCDIR, 'file-A')
    with open(fname, 'w') as file:
        file.write(filetime_a.strftime('%y%m%d'))
        pass
    timestamp = filetime_a.timestamp()
    os.utime(fname, (timestamp, timestamp))

    prepare_file_b(filetime_b)

    # finally reset time of root folder (was written during file operations above):
    timestamp = dirtime.timestamp()
    os.utime(SRCDIR, (timestamp, timestamp))


def prepare_file_b(filetime_b):
    fname = os.path.join(SRCDIR, 'subdir', 'file-B')
    if os.path.exists(fname):
        os.remove(fname)
    with open(fname, 'w') as file:
        file.write(filetime_b.strftime('%y%m%d'))
        pass
    timestamp = filetime_b.timestamp()
    os.utime(fname, (timestamp, timestamp))


def prepare_dstdir():
    if os.path.exists(DSTDIR):
        shutil.rmtree(DSTDIR)
    os.makedirs(DSTDIR)


def test_controller_create_snapshot():
    prepare_dstdir()

    dates = [datetime.datetime(2015, 1, 1), datetime.datetime(2015, 1, 3), datetime.datetime(2015, 1, 7), datetime.datetime(2015, 2, 1)]
    prepare_srcdir(datetime.datetime(2015, 1, 1), datetime.datetime(2015, 1, 1), dates[0])

    for date in dates[1:]:
        prepare_file_b(date)
        c = SnapshotController(SRCDIR, DSTDIR, [Queue('queue1', 2, 3)])
        c.create_snapshot()

    dirnames = os.listdir(DSTDIR)
    assert len(dirnames) == 3
    assert 'queue1-20150201000000' in dirnames
    assert 'queue1-20150107000000' in dirnames
    assert 'queue1-20150103000000' in dirnames

    # change source files to verify we have created hard links:
    with open(os.path.join(SRCDIR, 'file-A'), 'a') as file:
        file.write('modified')
    with open(os.path.join(SRCDIR, 'subdir', 'file-B'), 'a') as file:
        file.write('modified')

    with open(os.path.join(DSTDIR, 'queue1-20150201000000', 'file-A')) as file:
        text = file.read()
        assert text == '150101modified'
    with open(os.path.join(DSTDIR, 'queue1-20150107000000', 'file-A')) as file:
        text = file.read()
        assert text == '150101modified'
    with open(os.path.join(DSTDIR, 'queue1-20150103000000', 'file-A')) as file:
        text = file.read()
        assert text == '150101modified'

    with open(os.path.join(DSTDIR, 'queue1-20150201000000', 'subdir', 'file-B')) as file:
        text = file.read()
        assert text == '150201modified'
    with open(os.path.join(DSTDIR, 'queue1-20150107000000', 'subdir', 'file-B')) as file:
        text = file.read()
        assert text == '150107'
    with open(os.path.join(DSTDIR, 'queue1-20150103000000', 'subdir', 'file-B')) as file:
        text = file.read()
        assert text == '150103'
