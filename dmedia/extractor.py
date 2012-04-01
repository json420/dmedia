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
from subprocess import check_call, check_output, CalledProcessError
import json
import tempfile
import shutil
from base64 import b64encode
import time
import calendar
from collections import namedtuple

from filestore import hash_fp

import dmedia


dmedia_extract = 'dmedia-extract'
tree = path.dirname(path.dirname(path.abspath(dmedia.__file__)))
if path.isfile(path.join(tree, 'setup.py')):
    dmedia_extract = path.join(tree, dmedia_extract)


Thumbnail = namedtuple('Thumbnail', 'content_type data')

# Why 288x288 box for thumbnail size?  To preserve exact aspect ratio for
# common aspect-ratios we care about:
#   288x162 = 16:9
#   288x192 = 3:2
#   288x216 = 4:3
SIZE = 288


# exiftool adds some metadata that doesn't make sense to include:
EXIFTOOL_IGNORE = (
    'SourceFile',  # 'dmedia/tests/data/MVI_5751.THM'
    'ExifToolVersion',  # 8.15
    'FileName',  # 'MVI_5751.THM'
    'Directory',  # 'dmedia/tests/data',
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
    'camera_serial': ['SerialNumber'],
    'focal_length': ['FocalLength'],
}


#### Utility functions that do heavy lifting:

def extract_exif(filename):
    """
    Attempt to extract EXIF metadata from file at *filename*.
    """
    cmd = ['exiftool', '-j', filename]
    try:
        output = check_output(cmd)
    except CalledProcessError:
        return {}
    exif = json.loads(output.decode('utf-8'))[0]
    assert isinstance(exif, dict)
    for key in EXIFTOOL_IGNORE:
        exif.pop(key, None)
    return exif


def parse_subsec_datetime(string):
    """
    For example:

    >>> parse_subsec_datetime('2010:10:21 01:44:37.40')
    1287625477.4

    This function also works on timestamps without sub-seconds:

    >>> parse_subsec_datetime('2010:10:21 01:44:37')
    1287625477.0
    """
    if not isinstance(string, str):
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
    1287520994.68
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
        cmd = [dmedia_extract, filename]
        return json.loads(check_output(cmd).decode('utf-8'))
    except Exception:
        return {}


def extract(filename):
    """
    Extract video/audio/image properties using GStreamer.
    """
    try:
        cmd = [dmedia_extract, filename]
        return json.loads(check_output(cmd).decode('utf-8'))
    except Exception:
        return {}


def thumbnail_image(src, tmp):
    """
    Generate thumbnail for image with filename *src*.
    """
    dst = path.join(tmp, 'thumbnail.jpg')
    cmd = [
        'convert',
        src,
        '-scale', '{}x{}'.format(SIZE, SIZE), 
        '-unsharp', '3x2+0.5+0.0',
        '-strip',  # Remove EXIF and other metadata so thumbnail is smaller
        '-quality', '90', 
        dst,
    ]
    check_call(cmd)
    return Thumbnail('image/jpeg', open(dst, 'rb').read())


def thumbnail_video(src, tmp):
    """
    Generate thumbnail for video with filename *src*.
    """
    dst = path.join(tmp, 'frame.png')
    cmd = [
        'totem-video-thumbnailer',
        '-r',  # Create a "raw" thumbnail without film boarder
        '-s', str(SIZE),
        src,
        dst,
    ]
    check_call(cmd)
    return thumbnail_image(dst, tmp)


def thumbnail_raw(src, tmp):
    """
    Generate thumbnail for RAW photo with filename *src*.
    """
    dst = path.join(tmp, 'embedded.jpg')
    cmd = [
        'ufraw-batch',
        '--embedded-image',
        '--output', dst,
        src,
    ]
    check_call(cmd)
    return thumbnail_image(dst, tmp)


thumbnailers = {
    'mov': thumbnail_video,
    'cr2': thumbnail_raw,
    'jpg': thumbnail_image,
}


def create_thumbnail(filename, ext):
    if ext not in thumbnailers:
        return None
    tmp = tempfile.mkdtemp(prefix='dmedia.')
    try:
        return thumbnailers[ext](filename, tmp)
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


def merge_metadata(src, doc):
    ext = doc.get('ext')
    attachments = doc.get('_attachments', {})
    meta = doc.get('meta', {})
    if ext in _extractors:
        callback = _extractors[ext]
        for (key, value) in callback(src):
            if key == 'mtime':
                doc['ctime'] = value
            elif key not in meta:
                meta[key] = value
    thm = create_thumbnail(src, ext)
    if thm is not None:
        attachments['thumbnail'] = {
            'content_type': thm.content_type,
            'data': b64encode(thm.data).decode('utf-8'),
        }
    if attachments and '_attachments' not in doc:
        doc['_attachments'] = attachments
    if meta and 'meta' not in doc:
        doc['meta'] = meta
    if 'meta' in doc:
        for key in ('duration', 'framerate', 'samplerate'):
            if key in doc['meta']:
                doc[key] = doc['meta'][key]


def merge_exif(src):
    exif = extract_exif(src)
    for (key, values) in EXIF_REMAP.items():
        for v in values:
            if v in exif:
                yield (key, exif[v])
                break
    mtime = extract_mtime_from_exif(exif)
    if mtime is not None:
        yield ('mtime', mtime)

register(merge_exif, 'jpg', 'png', 'cr2')


def merge_video_info(src):
    info = extract_video_info(src)
    for item in info.items():
        yield item

    if not src.endswith('.MOV'):
        return

    # Extract EXIF metadata from Canon .THM file if present, otherwise try from
    # MOV (for 60D, T3i, etc):
    thm = src[:-3] + 'THM'
    if path.isfile(thm):
        ch = hash_fp(open(thm, 'rb'))
        yield ('canon_thm', ch.id)
    target = (thm if path.isfile(thm) else src)
    for (key, value) in merge_exif(target):
        if key in ('width', 'height'):
            continue
        yield (key, value)

register(merge_video_info, 'mov', 'mp4', 'avi', 'ogg', 'ogv', 'oga', 'mts', 'wav')
