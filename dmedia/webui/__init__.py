# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#
# dmedia: distributed media library
# Copyright (C) 2011 Jason Gerard DeRose <jderose@novacut.com>
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
Core HTML5 UI components.

This is used both when running a web-accesible dmedia server, and when running
an HTML5 UI in embedded WebKit.
"""

typemap = {
    'js': 'application/javascript',
    'json': 'application/json',
    'css': 'text/css',
    'bin': 'application/octet-stream',
    'png': 'image/png',
    'jpg': 'image/jpeg',
    'svg': 'image/svg+xml',
}

def get_mime(name, mime=None):
    if mime is not None:
        return mime
    ext = name.rsplit('.', 1)[-1].lower()
    return typemap.get(ext, 'application/octet-stream')


class App(object):
    def __init__(self):
        self._data = {}
        self._templates = {}

    def datafile(self, name, parent=None, mime=None):
        filename = path.join(parent, name)
        parent = (DATADIR if parent is None else parent)
        content_type = get_mime(name, mime)
        if name in self._data:
            d = self._data[name]
            assert d['filename'] == filename
            assert d['content_type'] == content_type
            return name
        self._data[name] = {
            'filename': filename,
            'content_type': content_type,
            'data': open(filename, 'rb').read(),
        }
        return name

    def template(self, name, parent=None):
        if name not in self._templates:
            self._templates[name] = template(name, parent)
        return self._templates[name]


class Page(object):
    top = ('top.xml', None)

    body = ('placeholder.xml', None)
