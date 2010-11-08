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

from distutils.core import setup
from distutils.cmd import Command
import dmedialib

cmdclass = {}

try:
    from unittest2 import TestLoader, TextTestRunner

    class Test(Command):
        user_options = []

        def run(self):
            loader = TestLoader()
            suite = loader.discover(dmedialib.packagedir)
            runner = TextTestRunner(verbosity=2)
            result = runner.run(suite)
            if not result.wasSuccessful():
                sys.exit(1)

        def initialize_options(self):
            pass

        def finalize_options(self):
            pass


    cmdclass['test'] = Test

except ImportError:
    pass


setup(
    name='dmedia',
    description='distributed media library',
    version=dmedialib.__version__,
    author='Jason Gerard DeRose',
    author_email='jderose@jasonderose.org',
    license='LGPLv3+',

    cmdclass=cmdclass,
    packages=['dmedialib'],
    package_data=dict(
        dmedialib=['data/*'],
    ),
    scripts=['dmedia', 'dmedia-gtk'],
)
