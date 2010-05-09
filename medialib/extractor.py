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
Extract meta-data from media files.
"""

import os
from os import path
from subprocess import check_call, Popen, PIPE
import json
from .filestore import normalize_ext


_extractors = {}


def register(callback, *extensions):
    for ext in extensions:
        _extractors[ext] = callback


def extract(filename, *pairs):
    name = path.basename(filename)
    ext = normalize_ext(name)
    if ext in _extractors:
        callback = _extractors[ext]
        for (key, value) in callback(filename):
            yield (key, value)
    yield ('bytes', path.getsize(filename))
    yield ('mtime', path.getmtime(filename))
    yield ('name', name)
    for (key, value) in pairs:
        yield (key, value)


_exif = {
    'width': ['ImageWidth'],
    'height': ['ImageHeight'],
    'iso': ['ISO'],
    'shutter': ['ShutterSpeed', 'ExposureTime'],
    'aperture': ['Aperture', 'FNumber', 'ApertureValue'],
    'focal_length': ['FocalLength', 'Lens'],
}

def extract_exif(filename):
    args = ['exiftool', '-j', filename]
    (stdout, stderr) = Popen(args, stdout=PIPE).communicate()
    d = json.loads(stdout)[0]
    for (key, sources) in _exif.iteritems():
        for src in sources:
            if src in d:
                yield (key, d[src])
                break


register(extract_exif, 'jpg', 'cr2')
