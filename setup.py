#!/usr/bin/env python3

# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#   David Green <david4dev@gmail.com>
#
# dmedia: distributed media library
# Copyright (C) 2010 Jason Gerard DeRose <jderose@novacut.com>
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under the
# terms of the GNU Affero General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# `dmedia` is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Affero General Public License for more
# details.
#
# You should have received a copy of the GNU Affero General Public License along
# with `dmedia`.  If not, see <http://www.gnu.org/licenses/>.

"""
Install `dmedia`
"""

import sys
if sys.version_info < (3, 3):
    sys.exit('ERROR: Dmedia requires Python 3.3 or newer')

import os
from os import path
import stat
from distutils.core import setup
from distutils.cmd import Command
from unittest import TestLoader, TextTestRunner, TestSuite
from doctest import DocTestSuite
import dmedia


packagedir = path.dirname(path.abspath(dmedia.__file__))


def pynames_iter(pkdir, pkname=None, core_only=False):
    """
    Recursively yield dotted names for *.py files in directory *pydir*.
    """
    if not path.isfile(path.join(pkdir, '__init__.py')):
        return
    if pkname is None:
        pkname = path.basename(pkdir)
    yield pkname
    dirs = []
    for name in sorted(os.listdir(pkdir)):
        if name in ('__init__.py', '__pycache__'):
            continue
        if name.startswith('.') or name.endswith('~'):
            continue
        fullname = path.join(pkdir, name)
        st = os.lstat(fullname)
        if stat.S_ISREG(st.st_mode) and name.endswith('.py'):
            parts = name.split('.')
            if len(parts) == 2:
                yield '.'.join([pkname, parts[0]])
        elif stat.S_ISDIR(st.st_mode):
            dirs.append((fullname, name))
    for (fullname, name) in dirs:
        subpkg = '.'.join([pkname, name])
        if core_only and subpkg != 'dmedia.tests':
            continue
        for n in pynames_iter(fullname, subpkg, core_only):
            yield n


class Test(Command):
    description = 'run unit tests and doc tests'

    user_options = [
        ('no-doctest', None, 'do not run doc-tests'),
        ('no-unittest', None, 'do not run unit-tests'),
        ('names=', None, 'comma-sperated list of modules to test'),
        ('core-only', None, 'only run core tests (no Gtk, no DBus)'),
    ]

    def _pynames_iter(self):
        for pyname in pynames_iter(packagedir, core_only=self.core_only):
            if not self.names:
                yield pyname
            else:
                for name in self.names:
                    if name in pyname:
                        yield pyname
                        break

    def run(self):
        pynames = list(self._pynames_iter())
        if self.core_only:
            os.environ['DMEDIA_TEST_CORE_ONLY'] = 'true'
            # FIXME: This is just till we drop totem-video-thumbnailer
            pynames.remove('dmedia.extractor')
            pynames.remove('dmedia.tests.test_extractor')

        # Add unit-tests:
        if self.no_unittest:
            suite = TestSuite()
        else:
            loader = TestLoader()
            suite = loader.loadTestsFromNames(pynames)

        # Add doc-tests:
        if not self.no_doctest:
            for mod in pynames:
                suite.addTest(DocTestSuite(mod))

        # Run the tests:
        runner = TextTestRunner(verbosity=2)
        result = runner.run(suite)
        if not result.wasSuccessful():
            raise SystemExit(1)

    def initialize_options(self):
        self.no_doctest = 0
        self.no_unittest = 0
        self.core_only = 0
        self.names = ''

    def finalize_options(self):
        self.names = self.names.split(',')


setup(
    name='dmedia',
    description='distributed media library',
    url='https://launchpad.net/dmedia',
    version=dmedia.__version__,
    author='Jason Gerard DeRose',
    author_email='jderose@novacut.com',
    license='AGPLv3+',
    cmdclass={'test': Test},
    packages=['dmedia', 'dmedia.service', 'dmedia.gtk'],
    package_data={'dmedia.gtk': ['ui/*']},
    scripts=[
        'dmedia-gtk',
        'dmedia-peer-gtk',
        'dmedia-cli',
        'dmedia-migrate',
        'dmedia-extract',
        'dmedia-v0-v1-upgrade',
        'dmedia-provision-drive',
    ],
    data_files=[
        ('share/couchdb/apps/dmedia',
            [path.join('ui', name) for name in os.listdir('ui')]
        ),
        ('share/applications', [
                'share/dmedia.desktop',
                'share/dmedia-peer.desktop'
            ]
        ),
        ('share/icons/hicolor/scalable/apps', [
                'share/dmedia.svg',
                'share/dmedia-peer.svg',
            ]
        ),
        ('share/icons/hicolor/scalable/status', [
                'share/indicator-dmedia-peer.svg',
                'share/indicator-dmedia.svg',
                'share/indicator-dmedia-att.svg',
            ]
        ),
        ('lib/dmedia', [
                'dmedia-service',
                'dbus-activation-hack.sh',
                'dmedia-transcoder',
            ]
        ),
        ('share/dbus-1/services/',
            ['share/org.freedesktop.Dmedia.service']
        ),
    ],
)
