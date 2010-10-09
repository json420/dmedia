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
Extract meta-data from media files.
"""

from subprocess import check_call, Popen, PIPE
import json


_extractors = {}


def register(callback, *extensions):
    for ext in extensions:
        _extractors[ext] = callback


def merge_metadata(d):
    src = d['src']
    meta = d['meta']
    ext = meta['ext']
    if ext in _extractors:
        callback = _extractors[ext]
        for (key, value) in callback(src):
            if key not in meta:
                meta[key] = value

_exif = {
    'width': ['ImageWidth'],
    'height': ['ImageHeight'],
    'iso': ['ISO'],
    'shutter': ['ShutterSpeed', 'ExposureTime'],
    'aperture': ['Aperture', 'FNumber', 'ApertureValue'],
    #'focal_length': ['FocalLength', 'Lens'],
}

def extract_exif(filename):
    args = ['exiftool', '-j', filename]
    (stdout, stderr) = Popen(args, stdout=PIPE).communicate()
    d = json.loads(stdout)[0]
    yield ('exif', d)
    for (key, sources) in _exif.iteritems():
        for src in sources:
            if src in d:
                yield (key, d[src])
                break

register(extract_exif, 'jpg', 'png', 'cr2')


_totem = {
    'duration': 'TOTEM_INFO_DURATION',
    'width': 'TOTEM_INFO_VIDEO_WIDTH',
    'height': 'TOTEM_INFO_VIDEO_HEIGHT',
    'codec_video': 'TOTEM_INFO_VIDEO_CODEC',
    'fps': 'TOTEM_INFO_FPS',
    'codec_audio': 'TOTEM_INFO_AUDIO_CODEC',
    'sample_rate': 'TOTEM_INFO_AUDIO_SAMPLE_RATE',
    'channels': 'TOTEM_INFO_AUDIO_CHANNELS',
}

def _parse_totem(stdout):
    for line in stdout.splitlines():
        pair = line.split('=', 1)
        if len(pair) != 2:
            continue
        (key, value) = pair
        try:
            value = int(value)
        except ValueError:
            pass
        yield (key, value)

def extract_totem(filename):
    args = ['totem-video-indexer', filename]
    (stdout, stderr) = Popen(args, stdout=PIPE).communicate()
    d = dict(_parse_totem(stdout))
    for (key, src) in _totem.iteritems():
        if src in d:
            yield (key, d[src])

register(extract_totem, 'mov', 'mp4', 'avi', 'ogg', 'ogv', 'oga')
