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

# Reduce function to both count and sum in a single view (thanks manveru!)
# However, this is unusably slow in CouchDB 1.1.1.  Revisit in 1.2.0.
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
    },
    'filters': {
        'normal': filter_doc_normal,
        'type': filter_doc_type,
    },
}



# The _design/file design, for dmedia/file docs:
file_stored = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        var key;
        for (key in doc.stored) {
            emit(key, null);
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
            if (doc.stored[key].copies) {
                copies += doc.stored[key].copies;
            }
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
            if (doc.stored[key].copies) {
                copies += doc.stored[key].copies;
            }
        }
        if (copies >= 3) {
            for (key in doc.stored) {
                if (copies - doc.stored[key].copies >= 3) {
                    emit([key, doc.atime], null);
                }
            }
        }
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

file_design = {
    '_id': '_design/file',
    'views': {
        'stored': {'map': file_stored},
        'fragile': {'map': file_fragile},
        'reclaimable': {'map': file_reclaimable},
        'verified': {'map': file_verified},
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

user_ctime = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.origin == 'user' && doc.ext != 'thm') {
        emit(doc.ctime, null);
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

user_design = {
    '_id': '_design/user',
    'views': {
        'ctime': {'map': user_ctime},
        'video': {'map': user_video},
        'audio': {'map': user_audio},
        'image': {'map': user_image},
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

project_design = {
    '_id': '_design/project',
    'views': {
        'atime': {'map': project_atime},
        'title': {'map': project_title},
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
