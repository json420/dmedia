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

from microfiber import NotFound


log = logging.getLogger()


# High performance Erlang reduce functions that don't require JSON round trip:
_count = '_count'
_sum = '_sum'

# Reduce function to both count and sum in a single view (thanks manveru!)
# Use with care, this is much, much slower than the above!
_count_and_sum = """
function(key, values, rereduce) {
    var count = 0;
    var sum = 0;
    var i;
    for (i in values) {
        count += values[i][0];
        sum += values[i][1];
    }
    return [count, sum];
}
"""


# The generic 'doc' design, quite helpful for developers:
doc_ver = """
function(doc) {
    emit(doc.ver, null);
}
"""

doc_type = """
function(doc) {
    emit(doc.type, null);
}
"""

doc_time = """
function(doc) {
    emit(doc.time, null);
}
"""

doc_design = ('doc', (
    ('ver', doc_ver, _count),
    ('type', doc_type, _count),
    ('time', doc_time, None),
))



# For dmedia/batch docs:
batch_time = """
function(doc) {
    if (doc.type == 'dmedia/batch') {
        emit(doc.time, null);
    }
}
"""

batch_design = ('batch', (
    ('time', batch_time, None),
))



# For dmedia/import docs:
import_time = """
function(doc) {
    if (doc.type == 'dmedia/import') {
        emit(doc.time, null);
    }
}
"""

import_design = ('import', (
    ('time', import_time, None),
))



# For dmedia/store docs:
store_plugin = """
function(doc) {
    if (doc.type == 'dmedia/store') {
        emit(doc.plugin, null);
    }
}
"""

store_design = ('store', (
    ('plugin', store_plugin, _count),
))



# For dmedia/file docs:
file_stored = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        var key;
        for (key in doc.stored) {
            emit(key, [1, doc.bytes]);
        }
    }
}
"""

file_origin = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        emit(doc.origin, [1, doc.bytes]);
    }
}
"""

file_verified = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        var key;
        for (key in doc.stored) {
            emit([key, doc.stored[key].verified], null);
        }
    }
}
"""

file_fragile = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        var copies = 0;
        var key;
        for (key in doc.stored) {
            copies += doc.stored[key].copies;
        }
        if (copies < 3) {
            emit(copies, null);
        }
    }
}
"""

file_reclaimable = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        var copies = 0;
        var key;
        for (key in doc.stored) {
            copies += doc.stored[key].copies;
        }
        if (copies > 3) {
            for (key in doc.stored) {
                if (copies - doc.stored[key].copies >= 3) {
                    emit([key, doc.atime], null);
                }
            }
        }
    }
}
"""

file_corrupt = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        var key;
        for (key in doc.corrupt) {
            emit(key, doc.bytes);
        }
    }
}
"""

file_partial = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        var key;
        for (key in doc.partial) {
            emit(key, doc.bytes);
        }
    }
}
"""

file_bytes = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        emit(doc.bytes, doc.bytes);
    }
}
"""

file_ctime = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        emit(doc.ctime, null);
    }
}
"""

file_ext = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        emit(doc.ext, [1, doc.bytes]);
    }
}
"""



# For dmedia/file docs where origin is 'user':
user_copies = """
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

user_ctime = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user' && doc.ext != 'thm') {
        emit(doc.ctime, null);
    }
}
"""

user_needsproxy = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        if (doc.ext == 'mov' && !doc.proxies) {
            emit(doc.time, null);
        }
    }
}
"""

user_video = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        if (doc.ext == 'mov') {
            emit(doc.ctime, doc.review);
        }
    }
}
"""

user_video_needsreview = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        if (doc.ext == 'mov' && !doc.review) {
            emit(doc.ctime, null);
        }
    }
}
"""

user_audio = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        if (doc.ext == 'wav') {
            emit(doc.ctime, doc.bytes);
        }
    }
}
"""

user_photo = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        if (['cr2', 'jpg'].indexOf(doc.ext) >= 0) {
            emit(doc.ctime, doc.bytes);
        }
    }
}
"""



# For dmedia/project docs:
project_atime = """
function(doc) {
    if (doc.type == 'dmedia/project') {
        emit(doc.atime, doc.title);
    }
}
"""

project_title = """
function(doc) {
    if (doc.type == 'dmedia/project') {
        emit(doc.title, doc.atime);
    }
}
"""



# For dmedia/tag docs:
tag_key = """
function(doc) {
    if(doc.type == 'dmedia/tag') {
        emit(doc.key, doc.value);
    }
}
"""

tag_letters = """
function(doc) {
    if(doc.type == 'dmedia/tag' && doc.key) {
        var i;
        for (i=0; i<doc.key.length; i++) {
            emit(doc.key.slice(0, i + 1), doc.value);
        }
    }
}
"""


core = (
    doc_design,
    batch_design,
    import_design,
    store_design,

    ('file', (
        ('stored', file_stored, _count_and_sum),
        ('ext', file_ext, _count_and_sum),
        ('origin', file_origin, _count_and_sum),

        ('fragile', file_fragile, None),
        ('reclaimable', file_reclaimable, None),
        ('partial', file_partial, _sum),
        ('corrupt', file_corrupt, _sum),
        ('bytes', file_bytes, _sum),
        ('verified', file_verified, None),
        ('ctime', file_ctime, None),
    )),

    ('user', (
        ('copies', user_copies, None),
        ('tags', user_tags, _count),
        ('ctime', user_ctime, None),
        ('needsproxy', user_needsproxy, None),
        ('video', user_video, None),
        ('photo', user_photo, None),
        ('audio', user_audio, None),
    )),

    ('project', (
        ('atime', project_atime, None),
        ('title', project_title, None),
    )),
)


project = (
    doc_design,
    batch_design,
    import_design,

    ('file', (
        ('stored', file_stored, _count_and_sum),
        ('ext', file_ext, _count_and_sum),
        ('origin', file_origin, _count_and_sum),

        ('fragile', file_fragile, None),
        ('reclaimable', file_reclaimable, None),
        ('partial', file_partial, _sum),
        ('corrupt', file_corrupt, _sum),
        ('bytes', file_bytes, _sum),
        ('verified', file_verified, None),
        ('ctime', file_ctime, None),
    )),

    ('user', (
        ('copies', user_copies, None),
        ('tags', user_tags, _count),
        ('ctime', user_ctime, None),
        ('needsproxy', user_needsproxy, None),
        ('video', user_video, None),
        ('video_needsreview', user_video_needsreview, None),
        ('photo', user_photo, None),
        ('audio', user_audio, None),
    )),

    ('tag', (
        ('key', tag_key, _count),
        ('letters', tag_letters, None),
    )),
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
        old = db.get(doc['_id'])
        doc['_rev'] = old['_rev']
        if doc != old:
            db.save(doc)
            return 'changed'
        else:
            return 'same'
    except NotFound:
        db.save(doc)
        return 'new'


def init_views(db, designs=core):
    log.info('Initializing views in %r', db)
    for (name, views) in designs:
        doc = build_design_doc(name, views)
        update_design_doc(db, doc)
