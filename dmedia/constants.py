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

import mimetypes
mimetypes.init()

# Standard read/write buffer size:
CHUNK_SIZE = 2**20  # 1 MiB

# Size of leaves in tree-hash:
LEAF_SIZE = 8 * 2**20  # 8 MiB


# FileStore path compotents
TRANSFERS_DIR = 'transfers'  # downloads/uploads
IMPORTS_DIR = 'imports'  # eg importing from CF card


# Normalized file extension
EXT_PAT = '^[a-z0-9]+(\.[a-z0-9]+)?$'


# D-Bus releated:
BUS = 'org.freedesktop.DMedia'
INTERFACE = 'org.freedesktop.DMedia'

# Standard format for TypeError message:
TYPE_ERROR = '%s: need a %r; got a %r: %r'

# Stardard format for TypeError message when a callable is expected:
CALLABLE_ERROR = '%s: need a callable; got a %r: %r'

def get_extensions_for_type(general_type):
    """
    An iterator that yields the file extensions for files of a general type.
    eg. 'image'
    """
    for ext in mimetypes.types_map:
        if mimetypes.types_map[ext].split('/')[0] == general_type:
            yield ext.strip('.')

VIDEO = tuple(get_extensions_for_type('video'))

AUDIO = tuple(get_extensions_for_type('audio'))

IMAGE = tuple(get_extensions_for_type('image'))

EXTENSIONS = VIDEO + AUDIO + IMAGE

EXT_MAP = {
    'video': VIDEO,
    'audio': AUDIO,
    'image': IMAGE,
    'all': EXTENSIONS,
}
