"""Top-level control flow of snapshot creation."""
from snapshot import Organizer


class SnapshotController:
    """Main control class to be used by end-user."""

    def __init__(self, srcdir, dstdir, queues):
        self.organizer = Organizer(srcdir, dstdir, queues)

    def create_snapshot(self):
        snapshot = self.organizer.create_snapshot()
        self.organizer.push(snapshot)
