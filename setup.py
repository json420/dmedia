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
import dmedialib

setup(
    name='dmedia',
    description='distributed media library',
    version=dmedialib.__version__,
    author='Jason Gerard DeRose',
    author_email='jderose@jasonderose.org',
    license='LGPLv3+',
    packages=['dmedialib'],
    scripts=['dmedia'],
)
