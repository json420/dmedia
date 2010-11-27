# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
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

BUS = 'org.freedesktop.DMedia'
INTERFACE = 'org.freedesktop.DMedia'

TYPE_ERROR = '%s: need a %r; got a %r: %r'  # Standard TypeError message


VIDEO = (
    'ogv',  # video/ogg
    'webm',  # video/webm
    'mov', 'qt',  # video/quicktime
    'mp4',  # video/mp4
    'mpeg', 'mpg', 'mpe',  # video/mpeg
    'avi',  # video/x-msvideo
    'mpv', 'mkv',  # video/x-matroska
)

AUDIO = ('wav', 'oga', 'flac', 'spx', 'mp3')

IMAGE = ('jpg', 'png', 'cr2', 'crw', 'nef')

EXTENSIONS = VIDEO + AUDIO + IMAGE

EXT_MAP = {
    'video': VIDEO,
    'audio': AUDIO,
    'image': IMAGE,
    'all': EXTENSIONS,
}
