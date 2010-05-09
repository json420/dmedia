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
`medialib` - distributed media library
"""

__version__ = '0.0.1'


class Media(object):

    def add(self, filename):
        pass

    def hasfile(self, filename):
        pass

    def haskey(self, key):
        pass

    def resolve(self, key):
        pass

    def get(self, key, resolve=False):
        pass

    def update(self, entry):
        pass
