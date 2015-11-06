"""Top-level control flow of snapshot creation."""
import argparse
import logging

import sys

from psnapshot.snapshot import Organizer, Queue

_logger = logging.getLogger(__name__)


class SnapshotController:
    """Main control class to be used by end-user."""

    def __init__(self, srcdir, dstdir, queues):
        self.organizer = Organizer(srcdir, dstdir, queues)

    def create_snapshot(self):
        self.organizer.find_snapshots()
        snapshot = self.organizer.create_snapshot()
        if snapshot:
            self.organizer.push(snapshot)


def main():
    try:
        parser = argparse.ArgumentParser(description='Python version of rsnapshot, managing queues of hard-linked copies or an rsync destination folder.')
        parser.add_argument('srcdir', help='Source directory to create hard-linked copies from.')
        parser.add_argument('dstdir', help='Destination directory, where queues of copies are stored.')
        parser.add_argument('-q', '--queue',
                            help='Queue definition in the form <name>[<length>]+<delta>, where <name> is the name of the queue, <length> the max length and '
                                 '<delta> the number of days between queue entries. This argument can be used multiple times to define more than one queue. '
                                 'If not given the default queue setup is daily[7]+1, weekly[4]+7 and monthly[3]+28.', action='append',
                            default=['daily[7]+1', 'weekly[4]+7', 'monthly[3]+28'])
        parser.add_argument('-l', '--log-level', help='Logging output level.', choices=['ERROR', 'WARNING', 'INFO', 'DEBUG'], default='INFO')
        args = parser.parse_args()

        logging.basicConfig(level=args.log_level, format='%(asctime)s %(levelname)-7s %(name)s %(message)s')
        _logger.info('Storing {} in {}.'.format(args.srcdir, args.dstdir))
        controller = SnapshotController(args.srcdir, args.dstdir, [Queue.from_textual_spec(spec) for spec in args.queue])
        controller.create_snapshot()
    except Exception as ex:
        _logger.error('Failed: {}'.format(ex))
        sys.exit(-1)


if __name__ == '__main__':
    main()
