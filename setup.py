#!/usr/bin/env python

# Authors:
#   Jason Gerard DeRose <jderose@jasonderose.org>
#
# dmedia: distributed media library
# Copyright (C) 2010 Jason Gerard DeRose <jderose@jasonderose.org>
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# `dmedia` is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License along
# with `dmedia`.  If not, see <http://www.gnu.org/licenses/>.

"""
Install `dmedia`
"""

import sys
import os
from os import path
from distutils.core import setup
from distutils.cmd import Command
from unittest import TestLoader, TextTestRunner
from doctest import DocTestSuite
import dmedialib


def pynames(pkdir, pkname=None):
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
            for n in pynames(fullname, '.'.join([pkname, name])):
                yield n


class Test(Command):
    user_options = []

    def run(self):
        names = tuple(pynames(dmedialib.packagedir))
        loader = TestLoader()
        suite = loader.loadTestsFromNames(names)
        for mod in names:
            suite.addTest(DocTestSuite(mod))
        runner = TextTestRunner(verbosity=2)
        result = runner.run(suite)
        if not result.wasSuccessful():
            sys.exit(1)

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass


setup(
    name='dmedia',
    description='distributed media library',
    version=dmedialib.__version__,
    author='Jason Gerard DeRose',
    author_email='jderose@jasonderose.org',
    license='LGPLv3+',

    cmdclass={'test': Test},
    packages=['dmedialib'],
    package_data=dict(
        dmedialib=['data/*'],
    ),
    scripts=['dmedia', 'dmedia-gtk'],
)
