"""Setup script for psnapshot."""

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
import sys


class PyTestCommand(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import pytest

        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


setup(
    name='psnapshot',
    version='0.1.1',
    packages=find_packages(),
    entry_points={'console_scripts': ['psnapshot = psnapshot.control:main']},
    #install_requires=install_requires,
    tests_require=[
        'pytest'
    ],
    cmdclass={'test': PyTestCommand},
    url='https://github.com/moltob/psnapshot',
    license='',
    author='Mike Pagel',
    author_email='mike@mpagel.de',
    description='Python implementation rsnapshot-like hardlink based backups.'
)
