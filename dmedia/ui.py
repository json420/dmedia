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
Web UI for dmedia browser.
"""

import os
from os import path
import json
from base64 import b64encode
from . import datadir


def render_var(name, obj):
    """
    Render *obj* to JavaScript variable *name*.

    For example:

    >>> render_var('dmedia', dict(hello='world', foo='bar'))
    'var dmedia = {"foo": "bar", "hello": "world"};'
    """
    return 'var %s = %s;' % (name, json.dumps(obj, sort_keys=True))


def datafile(name):
    """
    Return absolute path of datafile named *name*.
    """
    return path.join(datadir, name)


def datafile_comment(name):
    """
    Returns JavaScript/CSS comment with source of inlined datafile.
    """
    return '/* %s */\n' % datafile(name)


def load_datafile(name):
    return open(datafile(name), 'rb').read()


def inline_datafile(name):
    return datafile_comment(name) + load_datafile(name)


def encode_datafile(name):
    """
    Read datafile *name* and return base64-encoded.
    """
    return b64encode(load_datafile(name))


def create_app():
    return {
        '_id': 'app',
        '_attachments': {
            'browser': {
                'data': encode_datafile('browser.html'),
                'content_type': 'text/html',
            },
            'style.css': {
                'data': encode_datafile('style.css'),
                'content_type': 'text/css',
            },
            'browser.js': {
                'data': encode_datafile('browser.js'),
                'content_type': 'application/javascript',
            },
            'search.png': {
                'data': encode_datafile('search.png'),
                'content_type': 'image/png',
            },
            'stars.png': {
                'data': encode_datafile('stars.png'),
                'content_type': 'image/png',
            },
        }
    }
