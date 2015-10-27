"""Source of snapshot."""
import datetime
import os
from exceptions import SourceDirError


class Source:
    """Source from which snapshot is taken."""

    def __init__(self, dirpath):
        self.dirpath = dirpath
        self._time = None

        if not os.path.exists(dirpath):
            raise SourceDirError('Source directory {} does not exist.'.format(dirpath))

    @property
    def time(self):
        """Timestamp of newest file in source directory."""

        if not self._time:
            for path, _, filenames in os.walk(self.dirpath):
                for filename in filenames:
                    filepath = os.path.join(path, filename)
                    time = datetime.datetime.fromtimestamp(os.path.getmtime(filepath))

                    # round off to secs:
                    time = datetime.datetime(year=time.year,
                                             month=time.month,
                                             day=time.day,
                                             hour=time.hour,
                                             minute=time.minute,
                                             second=time.second)

                    self._time = time

        return self._time
