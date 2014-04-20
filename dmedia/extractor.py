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
from subprocess import check_call, check_output
from subprocess import CalledProcessError, TimeoutExpired
import json
import tempfile
import shutil
import time
import calendar
import logging

from filestore import hash_fp
from microfiber import Attachment, encode_attachment

import dmedia


log = logging.getLogger()
dmedia_extract = 'dmedia-extract'
tree = path.dirname(path.dirname(path.abspath(dmedia.__file__)))
if path.isfile(path.join(tree, 'setup.py')):
    dmedia_extract = path.join(tree, dmedia_extract)


# Why 288x288 box for thumbnail size?  To preserve exact aspect ratio for
# common aspect-ratios we care about:
#   288x162 = 16:9
#   288x192 = 3:2
#   288x216 = 4:3
SIZE = 288


REMAP_EXIF_THM = (
    ('aperture',
        ('Aperture', 'FNumber', 'ApertureValue')
    ),
    ('iso',
        ('ISO',)
    ),
    ('shutter', 
        ('ShutterSpeed', 'ExposureTime')
    ),

    ('camera_serial', 
        ('SerialNumber',)
    ),
    ('camera',
        ('Model',)
    ),
    ('lens',
        ('LensID', 'LensType')
    ),
    ('focal_length',
        ('FocalLength',)
    ),
)

REMAP_EXIF = REMAP_EXIF_THM + (
    ('width', 
        ('ImageWidth',)
    ),
    ('height',
        ('ImageHeight',)
    ),
    ('content_type',
        ('MIMEType',)
    ),
)

EXIF_CTIME_KEYS = (
    'SubSecCreateDate',
    'SubSecDateTimeOriginal',
    'SubSecModifyDate',
)



def check_json(cmd, default):
    try:
        return json.loads(check_output(cmd, timeout=3).decode('utf-8'))
    except (TimeoutExpired, CalledProcessError):
        log.exception(repr(cmd))
        return default


#### RAW extractors that call a script with check_output()
def raw_exiftool_extract(filename):
    """
    Extract EXIF metadata using `exiftool`.
    """
    cmd = ['exiftool', '-j', filename]
    return check_json(cmd, [{}])[0]


def raw_gst_extract(filename):
    """
    Extract video/audio/image properties using GStreamer.

    Extractions is done using the `dmedia-extract` script.
    """
    cmd = [dmedia_extract, filename]
    return check_json(cmd, {})


#### EXIF related utility functions:
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


def ctime_from_exif(exif):
    """
    Attempt to extract accurate mtime from EXIF data in *exif*.

    For example:

    >>> exif = {'SubSecCreateDate': '2010:10:19 20:43:14.68'}
    >>> ctime_from_exif(exif)
    1287520994.68
    """
    for key in EXIF_CTIME_KEYS:
        if key in exif:
            value = parse_subsec_datetime(exif[key])
            if value is not None:
                return value
    return None


def iter_exif(exif, remap=REMAP_EXIF):
    for (key, values) in remap:
        for v in values:
            if v in exif:
                yield (key, exif[v])
                break
    ctime = ctime_from_exif(exif)
    if ctime is not None:
        yield ('ctime', ctime)


def merge_metadata(doc, items):
    for (key, value) in items:
        if key in ('width', 'height', 'ctime', 'content_type'):
            doc[key] = value
        else:
            doc['meta'][key] = value


def merge_exif(src, doc, remap=REMAP_EXIF):
    exif = raw_exiftool_extract(src)
    merge_metadata(doc, iter_exif(exif, remap))


def merge_mov_exif(src, doc):
    if not src.endswith('.MOV'):
        return
    # Extract EXIF metadata from Canon .THM file if present, otherwise try from
    # MOV (for 60D, T3i, etc):
    thm = src[:-3] + 'THM'
    if path.isfile(thm):
        ch = hash_fp(open(thm, 'rb'))
        doc['meta']['canon_thm'] = ch.id
    target = (thm if path.isfile(thm) else src)
    merge_exif(target, doc, REMAP_EXIF_THM)     


media_image = {
    'cr2': {
        'content_type': 'image/x-canon-cr2',
        'media': 'image',
    },
    'jpg': {
        'content_type': 'image/jpeg',
        'media': 'image',
    }
}


NO_EXTRACT = (None, 'bin', 'bmp', 'cfg', 'dat', 'fir', 'log', 'lut', 'thm')


def extract(src, doc):
    """
    Extract 'physical' properties and metadata
    """
    ext = doc.get('ext')
    # For performance and sanity, we don't try to extract files with no
    # extension, THM files, or files from Magic Lantern: 
    if ext in NO_EXTRACT:
        return
    if ext in ('cr2', 'jpg'):
        merge_exif(src, doc)
        doc['media'] = 'image'
    else:
        info = raw_gst_extract(src)
        doc.update(info)
        if src.endswith('.MOV'):
            merge_mov_exif(src, doc)



#### Thumbnailing functions
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
    check_call(cmd, timeout=3)
    return Attachment('image/jpeg', open(dst, 'rb').read())


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
    check_call(cmd, timeout=3)
    return thumbnail_image(dst, tmp)


def thumbnail_raw(src, tmp):
    """
    Generate thumbnail for RAW photo with filename *src*.
    """
    dst = path.join(tmp, 'embedded.jpg')
    cmd = [
        'ufraw-batch',
        '--embedded-image',
        '--noexif',
        '--size', str(SIZE),
        '--compression', '90',
        '--output', dst,
        src,
    ]
    check_call(cmd, timeout=3)
    return Attachment('image/jpeg', open(dst, 'rb').read())


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


def wrap_thumbnail_func(func, filename):
    tmp = tempfile.mkdtemp(prefix='dmedia.')
    try:
        return func(filename, tmp)
    except Exception:
        return None
    finally:
        if path.isdir(tmp):
            shutil.rmtree(tmp)


def get_thumbnail_func(doc):
    media = doc.get('media')
    if media not in ('video', 'image'):
        return None
    if media == 'video':
        return thumbnail_video
    elif doc.get('ext') in ('cr2',):
        return thumbnail_raw
    return thumbnail_image
    

def merge_thumbnail(src, doc):
    func = get_thumbnail_func(doc)
    if func is None:
        return False
    thm = wrap_thumbnail_func(func, src)
    if thm is None:
        return False
    doc['_attachments']['thumbnail'] = encode_attachment(thm)
    return True

