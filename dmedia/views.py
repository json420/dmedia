# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#   David Green <david4dev@gmail.com>
#
# dmedia: distributed media library
# Copyright (C) 2010, 2011 Jason Gerard DeRose <jderose@novacut.com>
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
Defines the dmedia CouchDB views.
"""

import logging

from couchdb import ResourceNotFound


log = logging.getLogger()


_sum = '_sum'
_count = '_count'

type_type = """
function(doc) {
    if (doc.type) {
        emit(doc.type, null);
    }
}
"""

batch_time = """
function(doc) {
    if (doc.type == 'dmedia/batch') {
        emit(doc.time, null);
    }
}
"""

import_time = """
function(doc) {
    if (doc.type == 'dmedia/import') {
        emit(doc.time, null);
    }
}
"""

import_partition = """
function(doc) {
    if (doc.type == 'dmedia/import') {
        emit(doc.partition_id, null);
    }
}
"""



# views in the 'store' design only index docs where doc.type == 'dmedia/store'
store_plugin = """
function(doc) {
    if (doc.type == 'dmedia/store') {
        emit(doc.plugin, null);
    }
}
"""

store_partition = """
function(doc) {
    if (doc.type == 'dmedia/store') {
        if (doc.plugin == 'filestore') {
            emit(doc.partition_id, null);
        }
    }
}
"""


# views in the 'file' design only index docs for which doc.type == 'dmedia/file'
file_stored = """
// Get list of all files on a given store, total bytes on that store
function(doc) {
    if (doc.type == 'dmedia/file') {
        var key;
        for (key in doc.stored) {
            emit(key, doc.bytes);
        }
    }
}
"""

file_bytes = """
function(doc) {
    if (doc.type == 'dmedia/file' && typeof(doc.bytes) == 'number') {
        emit(doc.bytes, doc.bytes);
    }
}
"""

file_ext = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        emit(doc.ext, null);
    }
}
"""

file_content_type = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        emit(doc.content_type, null);
    }
}
"""

file_mtime = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        emit(doc.mtime, null);
    }
}
"""

file_import_id = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.import_id) {
        emit(doc.import_id, null);
    }
}
"""

# views in the 'user' design only index docs for which doc.type == 'dmedia/file'
# and doc.origin == 'user'
user_copies = """
// Durability of user's personal files
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        var copies = 0;
        var key;
        for (key in doc.stored) {
            copies += doc.stored[key].copies;
        }
        emit(copies, null);
    }
}
"""

user_media = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        emit(doc.media, null);
    }
}
"""

user_tags = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user' && doc.tags) {
        var key;
        for (key in doc.tags) {
            emit(key, doc.tags[key]);
        }
    }
}
"""

user_all = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        emit(doc.mtime, null);
    }
}
"""

user_video = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        if (doc.media == 'video') {
            emit(doc.mtime, null);
        }
    }
}
"""

user_image = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        if (doc.media == 'image') {
            emit(doc.mtime, null);
        }
    }
}
"""

user_audio = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        if (doc.media == 'audio') {
            emit(doc.mtime, null);
        }
    }
}
"""

user_inbox = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        if (doc.status == null) {
            emit(doc.mtime, null);
        }
    }
}
"""

user_reject = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        if (doc.status == 'reject') {
            emit(doc.mtime, null);
        }
    }
}
"""

user_keep = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        if (doc.status == 'keep') {
            emit(doc.mtime, null);
        }
    }
}
"""


store_plugin = """
function(doc) {
    if (doc.type == 'dmedia/store') {
        emit(doc.plugin, null);
    }
}
"""

partition_uuid = """
function(doc) {
    if (doc.type == 'dmedia/partition') {
        emit(doc.uuid, null)
    }
}
"""

partition_drive = """
function(doc) {
    if (doc.type == 'dmedia/partition') {
        emit(doc.drive_id, null)
    }
}
"""

drive_serial = """
function(doc) {
    if (doc.type == 'dmedia/drive') {
        emit(doc.serial, null)
    }
}
"""


designs = (
    ('type', (
        ('type', type_type, _count),
    )),

    ('batch', (
        ('time', batch_time, None),
    )),

    ('import', (
        ('time', import_time, None),
        ('partition', import_partition, None)
    )),

    ('store', (
        ('plugin', store_plugin, _count),
        ('partition', store_partition, None)
    )),

    ('file', (
        ('stored', file_stored, _sum),
        ('import_id', file_import_id, None),
        ('bytes', file_bytes, _sum),
        ('ext', file_ext, _count),
        ('content_type', file_content_type, _count),
        ('mtime', file_mtime, None),
    )),

    ('user', (
        ('copies', user_copies, None),
        ('media', user_media, _count),
        ('tags', user_tags, _count),
        ('all', user_all, None),
        ('video', user_video, None),
        ('image', user_image, None),
        ('audio', user_audio, None),

        # Inbox workflow
        ('inbox', user_inbox, _count),
        ('reject', user_reject, _count),
        ('keep', user_keep, _count),
    )),

    ('store', (
        ('plugin', store_plugin, _count),
        ('partition', store_partition, None)
    )),

    ('partition', (
        ('uuid', partition_uuid, None),
        ('drive', partition_drive, None)
    )),

    ('drive', (
        ('serial', drive_serial, None),
    ))
)


def iter_views(views):
    for (name, map_, reduce_) in views:
        if reduce_ is None:
            yield (name, {'map': map_.strip()})
        else:
            yield (name, {'map': map_.strip(), 'reduce': reduce_.strip()})


def build_design_doc(design, views):
    doc = {
        '_id': '_design/' + design,
        'language': 'javascript',
        'views': dict(iter_views(views)),
    }
    return doc


def update_design_doc(db, doc):
    assert '_rev' not in doc
    try:
        old = db[doc['_id']]
        doc['_rev'] = old['_rev']
        if doc != old:
            db.save(doc)
            return 'changed'
        else:
            return 'same'
    except ResourceNotFound:
        db.save(doc)
        return 'new'


def init_views(db):
    log.info('Initializing views in %r', db)
    for (name, views) in designs:
        doc = build_design_doc(name, views)
        update_design_doc(db, doc)
