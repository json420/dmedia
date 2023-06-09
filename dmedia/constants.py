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
Various constants conveniently located in one place.
"""

# Standard read/write buffer size:
CHUNK_SIZE = 2**20  # 1 MiB

# Size of leaves in tree-hash:
LEAF_SIZE = 8 * 2**20  # 8 MiB


# FileStore path compotents
TRANSFERS_DIR = 'transfers'  # downloads/uploads
IMPORTS_DIR = 'imports'  # eg importing from CF card
WRITES_DIR = 'writes'  # eg transcoding or rendering


# Normalized file extension
EXT_PAT = '^[a-z0-9]+(\.[a-z0-9]+)?$'


# D-Bus releated:
BUS = 'org.freedesktop.Dmedia'
IFACE = BUS
DC_BUS = 'org.desktopcouch.CouchDB'
DC_INTERFACE = DC_BUS


# Standard format for TypeError message:
TYPE_ERROR = '{}: need a {!r}; got a {!r}: {!r}'

# Stardard format for TypeError message when a callable is expected:
CALLABLE_ERROR = '{}: need a callable; got a {!r}: {!r}'
