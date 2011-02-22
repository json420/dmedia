#!/usr/bin/env python

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
import os
from os import path
from distutils.core import setup
from distutils.cmd import Command
from unittest import TestLoader, TextTestRunner, TestSuite
from doctest import DocTestSuite
import dmedia


def pynames_iter(pkdir, pkname=None):
    """
    Recursively yield dotted names for *.py files in directory *pydir*.
    """
    try:
        names = sorted(os.listdir(pkdir))
    except StandardError:
        return
    if not path.isfile(path.join(pkdir, '__init__.py')):
        return
    if pkname is None:
        pkname = path.basename(pkdir)
    yield pkname
    for name in names:
        if name == '__init__.py':
            continue
        if name.startswith('.') or name.endswith('~'):
            continue
        fullname = path.join(pkdir, name)
        if path.islink(fullname):
            continue
        if path.isfile(fullname) and name.endswith('.py'):
            parts = name.split('.')
            if len(parts) == 2:
                yield '.'.join([pkname, parts[0]])
        elif path.isdir(fullname):
            for n in pynames_iter(fullname, '.'.join([pkname, name])):
                yield n


class Test(Command):
    description = 'run unit tests and doc tests'

    user_options = [
        ('no-doctest', None, 'do not run doc-tests'),
        ('no-unittest', None, 'do not run unit-tests'),
        ('names=', None, 'comma-sperated list of modules to test'),
    ]

    def _pynames_iter(self):
        for pyname in pynames_iter(dmedia.packagedir):
            if not self.names:
                yield pyname
            else:
                for name in self.names:
                    if name in pyname:
                        yield pyname
                        break

    def run(self):
        from dbus.mainloop.glib import DBusGMainLoop
        import gobject
        DBusGMainLoop(set_as_default=True)
        gobject.threads_init()

        pynames = tuple(self._pynames_iter())

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
            sys.exit(1)

    def initialize_options(self):
        self.no_doctest = 0
        self.no_unittest = 0
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
    packages=['dmedia'],
    package_data=dict(
        dmedia=['data/*'],
    ),
    scripts=['dmedia-cli', 'dmedia-import', 'dmedia-gtk'],
    data_files=[
        ('share/man/man1', ['data/dmedia-cli.1']),
        ('share/applications', ['data/dmedia-import.desktop']),
        #^ this enables Nautilus to use dmedia-import as a handler for
        #media devices such as cameras. `sudo update-desktop-database`
        #may need to run for this to show up in the Nautilus
        #media handling preferences.
        ('share/pixmaps', ['data/dmedia.svg']),
        ('share/pixmaps/dmedia',
            [
                'data/indicator-rendermenu.svg',
                'data/indicator-rendermenu-att.svg',
            ]
        ),
        ('share/icons/hicolor/scalable/status/'
            [
                'data/indicator-rendermenu.svg',
                'data/indicator-rendermenu-att.svg',
            ]
        ), #enables status icons to be referenced by icon name
        ('share/dbus-1/services', ['data/org.freedesktop.DMedia.service']),
        ('lib/dmedia', ['dmedia-service']),
    ],
)
