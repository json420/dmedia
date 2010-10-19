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

from os import path
from subprocess import check_call, Popen, PIPE
import json
import tempfile
import shutil
from base64 import b64encode

# exiftool adds some metadata that doesn't make sense to include:
EXIFTOOL_IGNORE = (
    'SourceFile',  # '/home/jderose/bzr/dmedia/dmedialib/tests/data/MVI_5751.THM'
    'Directory',  # '/home/jderose/bzr/dmedia/dmedialib/tests/data',
    'ExifToolVersion',  # 8.1500000000000004

    'FileModifyDate',  # '2010:10:19 20:43:18-06:00'
    'FileName',  # 'MVI_5751.THM'
    'FilePermissions',  # 'rw-r--r--'
    'FileSize',  # '27 kB'
    'FileType',  # 'JPEG'
)


def file_2_base64(filename):
    """
    Return contents of file at *filename* base64-encoded.
    """
    return b64encode(open(filename, 'rb').read())


_extractors = {}


def register(callback, *extensions):
    for ext in extensions:
        _extractors[ext] = callback


def merge_metadata(d):
    meta = d['meta']
    ext = meta['ext']
    if ext in _extractors:
        callback = _extractors[ext]
        for (key, value) in callback(d):
            if key not in meta:
                meta[key] = value

_exif = {
    'width': ['ImageWidth'],
    'height': ['ImageHeight'],
    'iso': ['ISO'],
    'shutter': ['ShutterSpeed', 'ExposureTime'],
    'aperture': ['Aperture', 'FNumber', 'ApertureValue'],
    'lens': ['LensID', 'LensType'],
    'camera': ['Model'],
    'focal_length': ['FocalLength', 'Lens'],
}


def extract_exif(src):
    """
    Attempt to extract EXIF metadata from file *src*.
    """
    try:
        args = ['exiftool', '-j', src]
        (stdout, stderr) = Popen(args, stdout=PIPE).communicate()
        exif = json.loads(stdout)[0]
        assert isinstance(exif, dict)
        for key in EXIFTOOL_IGNORE:
            exif.pop(key, None)
        return exif
    except Exception as e:
        return {u'Error': u'%s: %s' % (e.__class__.__name__, e)}


def merge_exif(d):
    exif = extract_exif(d['src'])
    yield ('exif', exif)
    for (key, sources) in _exif.iteritems():
        for src in sources:
            if src in exif:
                yield (key, exif[src])
                break

register(merge_exif, 'jpg', 'png', 'cr2')


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


def extract_thumbnail(src):
    tmp = tempfile.mkdtemp(prefix='dmedia.')
    dst = path.join(tmp, 'thumbnail.jpg')
    check_call([
        'totem-video-thumbnailer',
        '-r', # Create a "raw" thumbnail without film boarder
        '-j', # Save as JPEG instead of PNG
        '-s', '192', # Fit video into 192x192 pixel square (192x108 for 16:9)
        src,
        dst,
    ])
    ret = {
        'content_type': 'image/jpeg',
        'data': file_2_base64(dst),
    }
    shutil.rmtree(tmp)
    return ret


def extract_totem(d):
    filename = d['src']
    args = ['totem-video-indexer', filename]
    (stdout, stderr) = Popen(args, stdout=PIPE).communicate()
    meta = dict(_parse_totem(stdout))
    for (key, src) in _totem.iteritems():
        if src in meta:
            yield (key, meta[src])
    try:
        yield (
            '_attachments',
            {'thumbnail': extract_thumbnail(filename)}
        )
    except Exception:
        pass
    if d['meta']['ext'] != 'mov':
        return
    thm = path.join(d['base'], d['root'] + '.THM')
    if path.isfile(thm):
        for (key, value) in merge_exif({'src': thm}):
            yield (key, value)

register(extract_totem, 'mov', 'mp4', 'avi', 'ogg', 'ogv', 'oga')
