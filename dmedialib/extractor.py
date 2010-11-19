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
Extract meta-data from media files.
"""

from os import path
from subprocess import check_call, Popen, PIPE
import json
import tempfile
import shutil
from base64 import b64encode
import time
import calendar

# exiftool adds some metadata that doesn't make sense to include:
EXIFTOOL_IGNORE = (
    'SourceFile',  # 'dmedialib/tests/data/MVI_5751.THM'
    'ExifToolVersion',  # 8.15
    'FileName',  # 'MVI_5751.THM'
    'Directory',  # 'dmedialib/tests/data',
    'FileSize',  # '27 kB'
    'FileModifyDate',  # '2010:10:19 20:43:18-06:00'
    'FilePermissions',  # 'rw-r--r--'
    'FileType',  # 'JPEG'
    'MIMEType',  # 'image/jpeg'
    'ExifByteOrder',  # 'Little-endian (Intel, II)'
)


# We try to pull an authoritative mtime from these EXIF keys:
EXIF_MTIME_KEYS = (
    'SubSecCreateDate',
    'SubSecDateTimeOriginal',
    'SubSecModifyDate',
)


# Store some EXIF data using standardized keys in document:
EXIF_REMAP = {
    'width': ['ImageWidth'],
    'height': ['ImageHeight'],
    'iso': ['ISO'],
    'shutter': ['ShutterSpeed', 'ExposureTime'],
    'aperture': ['Aperture', 'FNumber', 'ApertureValue'],
    'lens': ['LensID', 'LensType'],
    'camera': ['Model'],
    'focal_length': ['FocalLength'],
}


TOTEM_REMAP = (
    ('duration', 'TOTEM_INFO_DURATION'),
    ('width', 'TOTEM_INFO_VIDEO_WIDTH'),
    ('height', 'TOTEM_INFO_VIDEO_HEIGHT'),
    ('codec_video', 'TOTEM_INFO_VIDEO_CODEC'),
    ('fps', 'TOTEM_INFO_FPS'),
    ('codec_audio', 'TOTEM_INFO_AUDIO_CODEC'),
    ('sample_rate', 'TOTEM_INFO_AUDIO_SAMPLE_RATE'),
    ('channels', 'TOTEM_INFO_AUDIO_CHANNELS'),
)


#### Utility functions that do heavy lifting:

def file_2_base64(filename):
    """
    Return contents of file at *filename* base64-encoded.
    """
    return b64encode(open(filename, 'rb').read())


def extract_exif(filename):
    """
    Attempt to extract EXIF metadata from file at *filename*.
    """
    try:
        args = ['exiftool', '-j', filename]
        (stdout, stderr) = Popen(args, stdout=PIPE).communicate()
        exif = json.loads(stdout)[0]
        assert isinstance(exif, dict)
        for key in EXIFTOOL_IGNORE:
            exif.pop(key, None)
        return exif
    except Exception as e:
        return {u'Error': u'%s: %s' % (e.__class__.__name__, e)}


def parse_subsec_datetime(string):
    """
    For example:

    >>> parse_subsec_datetime('2010:10:21 01:44:37.40')
    1287625477.4000001

    This function also works on timestamps without sub-seconds:

    >>> parse_subsec_datetime('2010:10:21 01:44:37')
    1287625477.0
    """
    if not isinstance(string, basestring):
        return
    parts = string.split('.')
    if len(parts) == 1:
        stamp = parts[0]
        subsec = '00'
    elif len(parts) == 2:
        (stamp, subsec) = parts
    else:
        return
    if len(stamp) != 19 or len(subsec) != 2:
        return
    try:
        struct_time = time.strptime(stamp, '%Y:%m:%d %H:%M:%S')
        subsec = int(subsec)
        if not (0 <= subsec < 100):
            return
        hundredths = subsec / 100.0
    except ValueError:
        return
    return calendar.timegm(struct_time) + hundredths


def extract_mtime_from_exif(exif):
    """
    Attempt to extract accurate mtime from EXIF data in *exif*.

    For example:

    >>> exif = {'SubSecCreateDate': '2010:10:19 20:43:14.68'}
    >>> extract_mtime_from_exif(exif)
    1287520994.6800001
    """
    for key in EXIF_MTIME_KEYS:
        if key in exif:
            value = parse_subsec_datetime(exif[key])
            if value is not None:
                return value
    return None


def extract_video_info(filename):
    """
    Attempt to extract video metadata from video at *filename*.
    """
    try:
        args = ['totem-video-indexer', filename]
        popen = Popen(args, stdout=PIPE)
        (stdout, stderr) = popen.communicate()
        if popen.returncode != 0:
            return {}
        info = {}
        for line in stdout.splitlines():
            pair = line.split('=', 1)
            if len(pair) != 2:
                continue
            (key, value) = pair
            info[key] = value
        return info
    except Exception:
        return {}


def generate_thumbnail(filename):
    """
    Generate thumbnail for video at *filename*.
    """
    try:
        tmp = tempfile.mkdtemp(prefix='dmedia.')
        dst = path.join(tmp, 'thumbnail.jpg')
        check_call([
            'totem-video-thumbnailer',
            '-r', # Create a "raw" thumbnail without film boarder
            '-j', # Save as JPEG instead of PNG
            '-s', '192', # Fit video into 192x192 pixel square (192x108 for 16:9)
            filename,
            dst,
        ])
        return {
            'content_type': 'image/jpeg',
            'data': file_2_base64(dst),
        }
    except Exception:
        return None
    finally:
        if path.isdir(tmp):
            shutil.rmtree(tmp)



#### High-level meta-data extract/merge functions:

_extractors = {}


def register(callback, *extensions):
    assert callable(callback)
    for ext in extensions:
        assert isinstance(ext, str)
        _extractors[ext] = callback


def merge_metadata(d):
    doc = d['doc']
    ext = doc['ext']
    if ext in _extractors:
        callback = _extractors[ext]
        for (key, value) in callback(d):
            if key not in doc or key == 'mtime':
                doc[key] = value


def merge_exif(d):
    filename = d['src']
    exif = extract_exif(filename)
    for (key, sources) in EXIF_REMAP.iteritems():
        for src in sources:
            if src in exif:
                yield (key, exif[src])
                break
    mtime = extract_mtime_from_exif(exif)
    if mtime is not None:
        yield ('mtime', mtime)


register(merge_exif, 'jpg', 'png', 'cr2')


def merge_video_info(d):
    filename = d['src']
    info = extract_video_info(filename)
    for (dst_key, src_key) in TOTEM_REMAP:
        if src_key in info:
            value = info[src_key]
            try:
                value = int(value)
            except ValueError:
                pass
            yield (dst_key, value)

    # Try to generate thumbnail:
    attachments = {}
    yield ('_attachments', attachments)
    thumbnail = generate_thumbnail(filename)
    if thumbnail is not None:
        attachments['thumbnail'] = thumbnail

    if d['doc']['ext'] != 'mov':
        return

    # Extract EXIF metadata from Canon .THM file if present:
    thm = path.join(d['base'], d['root'] + '.THM')
    if path.isfile(thm):
        attachments['canon.thm'] = {
            'content_type': 'image/jpeg',
            'data': file_2_base64(thm),
        }
        for (key, value) in merge_exif({'src': thm}):
            if key in ('width', 'height'):
                continue
            yield (key, value)

register(merge_video_info, 'mov', 'mp4', 'avi', 'ogg', 'ogv', 'oga')
