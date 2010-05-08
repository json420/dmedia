#!/usr/bin/env python

# Authors:
#   Jason Gerard DeRose <jderose@jasonderose.org>
# 
# media: distributed media library
# Copyright (C) 2010 Jason Gerard DeRose <jderose@jasonderose.org>
# 
# This file is part of `media`.
# 
# `media` is free software: you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
# 
# `media` is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
# details.
# 
# You should have received a copy of the GNU Lesser General Public License along
# with `media`.  If not, see <http://www.gnu.org/licenses/>.

"""
Install `media`
"""

from distutils.core import setup
import medialib

setup(
    name='media',
    description='distributed media library',
    version=medialib.__version__,
    author='Jason Gerard DeRose',
    author_email='jderose@jasonderose.org',
    license='LGPLv3+',
    packages=['medialib'],
    scripts=['media'],
)
