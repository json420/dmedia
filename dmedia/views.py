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


# High performance Erlang reduce functions that don't require JSON round trip:
_count = '_count'
_sum = '_sum'
_stats = '_stats'


# The generic _design/doc design, quite helpful for developers:
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

# For confirming assumptions about CouchDB view sort order:
doc_key = """
function(doc) {
    emit(doc.key, null);
}
"""

filter_doc_normal = """
function(doc, req) {
    return doc._id[0] != '_';
}
"""

filter_doc_type = """
function(doc, req) {
    if (doc.type && doc.type == req.query.value) {
        return true;
    }
    return false;
}
"""

doc_design = {
    '_id': '_design/doc',
    'views': {
        'type': {'map': doc_type, 'reduce': _count},
        'time': {'map': doc_time},
        'key': {'map': doc_key},
    },
    'filters': {
        'normal': filter_doc_normal,
        'type': filter_doc_type,
    },
}



# The file design, for dmedia/file docs:
file_stored = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        var key;
        var bytes = (typeof doc.bytes == 'number') ? doc.bytes : 0;
        for (key in doc.stored) {
            emit(key, bytes);
        }
    }
}
"""

file_nonzero = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        var key;
        for (key in doc.stored) {
            if (doc.stored[key].copies !== 0) {
                emit(key, null);
            }
        }
    }
}
"""

file_copies = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        var total = 0;
        var key, copies;
        for (key in doc.stored) {
            copies = doc.stored[key].copies;
            if (typeof copies == 'number' && copies > 0) {
                total += copies;
            }
        }
        emit(total, null);
    }
}
"""

file_rank = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        var locations = 0;
        var durability = 0;
        var key, copies;
        for (key in doc.stored) {
            copies = doc.stored[key].copies;
            locations += 1;
            if (typeof copies == 'number' && copies > 0) {
                durability += copies;
            }
        }
        emit(Math.min(3, locations) + durability, null);
    }
}
"""

file_fragile = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        var total = 0;
        var key, copies;
        for (key in doc.stored) {
            copies = doc.stored[key].copies;
            if (typeof copies == 'number' && copies > 0) {
                total += copies;
            }
        }
        if (total < 3) {
            emit(total, null);
        }
    }
}
"""

filter_file_fragile = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        var total = 0;
        var key, copies;
        for (key in doc.stored) {
            copies = doc.stored[key].copies;
            if (typeof copies == 'number' && copies > 0) {
                total += copies;
            }
        }
        if (total < 3) {
            return true;
        }
        return false;
    }
}
"""

# Drives downgrading: files that have never been verified and are not already
# downgraded, ordered by mtime:
file_never_verified = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        var key, value, mtime;
        for (key in doc.stored) {
            value = doc.stored[key];
            if (typeof value.verified != 'number' && value.copies !== 0) {
                mtime = (typeof value.mtime == 'number') ? value.mtime : null;
                emit(mtime, key);
            }
        }
    }
}
"""

# Drives downgrading: files with a numeric `verified` than have not already
# been downgraded, ordered by verified:
file_last_verified = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        var key, value;
        for (key in doc.stored) {
            value = doc.stored[key];
            if (typeof value.verified == 'number' && value.copies !== 0) {
                emit(value.verified, key);
            }
        }
    }
}
"""

# Files that are in a downgraded state, ordered by store_id:
file_store_downgraded = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        var key, value;
        for (key in doc.stored) {
            value = doc.stored[key];
            if (typeof value.verified != 'number' && value.copies === 0) {
                emit(key, null);
            }
        }
    }
}
"""

# Files that have never been verified, ordered by [store_id, mtime]:
file_store_mtime = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        var key, value, mtime;
        for (key in doc.stored) {
            value = doc.stored[key];
            if (typeof value.verified != 'number' && value.copies !== 0) {
                mtime = (typeof value.mtime == 'number') ? value.mtime : null;
                emit([key, mtime], null);
            }
        }
    }
}
"""

# Files that have previously been verified, ordered by [store_id, verified]:
file_store_verified = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        var key, value;
        for (key in doc.stored) {
            value = doc.stored[key];
            if (typeof value.verified == 'number') {
                emit([key, value.verified], null);
            }
        }
    }
}
"""

# Files that can be reclaimed, ordered by [store_id, atime]:
file_store_reclaimable = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        var atime = (typeof doc.atime == 'number') ? doc.atime : null;
        var total = 0;
        var key, value, copies;
        for (key in doc.stored) {
            value = doc.stored[key];
            copies = (typeof value.copies == 'number') ? value.copies : 0;
            total += Math.max(0, copies);
        }
        if (total >= 3) {
            for (key in doc.stored) {
                value = doc.stored[key];
                copies = (typeof value.copies == 'number') ? value.copies : 0;
                if (total - copies >= 3 && !value.pinned) {
                    emit([key, atime], null);
                }
            }
        }
    }
}
"""

file_origin = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        var bytes = (typeof doc.bytes == 'number') ? doc.bytes : 0;
        emit(doc.origin, bytes);
    }
}
"""

file_design = {
    '_id': '_design/file',
    'views': {
        'stored': {'map': file_stored, 'reduce': _stats},
        'nonzero': {'map': file_nonzero},
        'copies': {'map': file_copies},
        'rank': {'map': file_rank},
        'fragile': {'map': file_fragile},
        'never-verified': {'map': file_never_verified},
        'last-verified': {'map': file_last_verified},
        'store-downgraded': {'map': file_store_downgraded},
        'store-mtime': {'map': file_store_mtime},
        'store-verified': {'map': file_store_verified},
        'store-reclaimable': {'map': file_store_reclaimable},
        'origin': {'map': file_origin, 'reduce': _stats},
    },
    'filters': {
        'fragile': filter_file_fragile,
    },
}


# For dmedia/store docs
store_atime = """
function(doc) {
    if (doc.type == 'dmedia/store') {
        if (typeof doc.atime == 'number') {
            emit(doc.atime, null);
        }
        else {
            emit(doc.time, null);
        }
    }
}
"""

store_drive_serial = """
function(doc) {
    if (doc.type == 'dmedia/store') {
        emit(doc.drive_serial, null);
    }
}
"""

store_design = {
    '_id': '_design/store',
    'views': {
        'atime': {'map': store_atime},
        'drive_serial': {'map': store_drive_serial},
    },
}


# The _design/user design, for dmedia/file docs where origin == 'user':
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

user_bytes = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        emit(doc.bytes, doc.bytes);
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
        if (doc.media == 'video' && !doc.proxies) {
            emit(doc.time, null);
        }
    }
}
"""

user_video = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        if (doc.media == 'video') {
            emit(doc.ctime, doc.review);
        }
    }
}
"""

user_video_needsreview = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        if (doc.media == 'video' && !doc.review) {
            emit(doc.ctime, null);
        }
    }
}
"""

user_audio = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        if (doc.media == 'audio') {
            emit(doc.ctime, doc.review);
        }
    }
}
"""

user_image = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        if (doc.media == 'image') {
            emit(doc.ctime, doc.review);
        }
    }
}
"""

user_thumbnail = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        if (doc._attachments && doc._attachments.thumbnail) {
            emit(doc.time, null);
        }
    }
}
"""

user_design = {
    '_id': '_design/user',
    'views': {
        'ctime': {'map': user_ctime},
        'bytes': {'map': user_bytes, 'reduce': _stats},
        'needsproxy': {'map': user_needsproxy},
        'video': {'map': user_video},
        'video_needsreview': {'map': user_video_needsreview},
        'audio': {'map': user_audio},
        'image': {'map': user_image},
        'thumbnail': {'map': user_thumbnail},
    },
}


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

filter_project_type = """
function(doc, req) {
    if (doc.type == 'dmedia/project') {
        return true;
    }
    return false;
}
"""

project_history = """
function(doc) {
    if (doc.type == 'dmedia/import' && doc.time_end) {
        var value = {
            'label': doc.partition.label,
            'size': doc.partition.size,
            'bytes': doc.stats.total.bytes,
            'count': doc.stats.total.count,
            'rate': doc.rate,
        }
        emit(doc.time, value);
    }
}
"""

filter_project_history = """
function(doc, req) {
    if (doc.type == 'dmedia/import' && doc.time_end) {
        return true;
    }
    return false;
}
"""

project_design = {
    '_id': '_design/project',
    'views': {
        'atime': {'map': project_atime},
        'title': {'map': project_title},
        'history': {'map': project_history},
    },
    'filters': {
        'type': filter_project_type,
        'history': filter_project_history,
    },
}


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

tag_design = {
    '_id': '_design/tag',
    'views': {
        'key': {'map': tag_key, 'reduce': _count},
        'letters': {'map': tag_letters},
    },
}



# For dmedia/file docs in a project
media_framerate = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user' && doc.framerate) {
        emit(doc.framerate, null);   
    }
}
"""

media_samplerate = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user' && doc.samplerate) {
        emit(doc.samplerate, null); 
    }
}
"""

media_size = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        if (doc.width && doc.height) {
            emit([doc.width, doc.height].join('x'), null);
        }
    }
}
"""

media_seconds = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user') {
        if (doc.duration && doc.duration.seconds) {
            emit(doc.meta.camera_serial, doc.duration.seconds);
        }
    }
}
"""

media_design = {
    '_id': '_design/media',
    'views': {
        'framerate': {'map': media_framerate, 'reduce': _count},
        'samplerate': {'map': media_samplerate, 'reduce': _count},
        'size': {'map': media_size, 'reduce': _count},
        'seconds': {'map': media_seconds, 'reduce': _sum},
    },
}


# For dmedia/file doc with interesting camera/photographic metadata
camera_serial = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user' && doc.meta) {
        if (doc.meta.camera_serial) {
            emit(doc.meta.camera_serial, null);
        }
    }
}
"""

camera_model = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user' && doc.meta) {
        if (doc.meta.camera) {
            emit(doc.meta.camera, null);
        }
    }
}
"""

camera_lens = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user' && doc.meta) {
        if (doc.meta.lens) {
            emit(doc.meta.lens, null);
        }
    }
}
"""

camera_aperture = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user' && doc.meta) {
        if (doc.meta.aperture) {
            emit(doc.meta.aperture, null);
        }
    }
}
"""

camera_shutter = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user' && doc.meta) {
        if (doc.meta.shutter) {
            emit(doc.meta.shutter, null);
        }
    }
}
"""

camera_iso = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user' && doc.meta) {
        if (doc.meta.iso) {
            emit(doc.meta.iso, null);
        }
    }
}
"""

camera_design = {
    '_id': '_design/camera',
    'views': {
        'model': {'map': camera_model, 'reduce': _count},
        'serial': {'map': camera_serial, 'reduce': _count},
        'lens': {'map': camera_lens, 'reduce': _count},
        'aperture': {'map': camera_aperture, 'reduce': _count},
        'shutter': {'map': camera_shutter, 'reduce': _count},
        'iso': {'map': camera_iso, 'reduce': _count},
    },
}


# For dmedia/job docs
job_waiting = """
function(doc) {
    if (doc.type == 'dmedia/job' && doc.status == 'waiting') {
        emit(doc.time, null);
    }
}
"""

job_design = {
    '_id': '_design/job',
    'views': {
        'waiting': {'map': job_waiting},
    }
}



core = (
    doc_design,
    file_design,
    store_design,
    project_design,
    job_design,
)


project = (
    doc_design,
    user_design,
    tag_design,
    media_design,
    camera_design,
)
