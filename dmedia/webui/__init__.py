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

from util import load_template, fullpath

typemap = {
    'js': 'application/javascript; charset=UTF-8',
    'json': 'application/json',
    'css': 'text/css; charset=UTF-8',
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
        filename = fullpath(name, parent)
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
        filename = fullpath(name, parent)
        try:
            return self._templates[filename]
        except KeyError:
            t = load_template(filename)
            self._templates[filename] = t
            return t

class Page(object):
    def __init__(self, app):
        self.app = app

    # You probably dont want to change these:
    content_type = 'text/html; charset=UTF-8'
    serializer = 'xml'
    doctype = 'html5'
    top = ('top.xml', None)  # Top level template

    # Definitely do want to change these:
    name = 'page'
    title = 'To change, override `Page.title`'
    body = ('placeholder.xml', None)  # The <body>...</body> template

    # CSS:
    css = (
        ('base.css', None),
    )
    inline_css = None

    # JavaScript:
    js = (
        ('dmedia.js', None),
    )
    inline_js = None

    def render(self):
        body = self.app.template(*self.body)
        d = dict(
            content_type=self.content_type,
            title=self.title,
            css=self.get_css(),
            inline_css=self.get_inline_css(),
            js=self.get_js(),
            inline_js=self.get_inline_js(),
            body=body.generate(**self.get_body_vars()),
        )
        top = self.app.template(*self.top)
        return top.generate(**d).render(self.serializer, doctype=self.doctype)

    def get_css(self):
        if not self.css:
            return tuple()
        t = typemap['css']
        return tuple(
            self.app.datafile(name, parent, t) for (name, parent) in self.css,
        )

    def get_inline_css(self):
        return self.inline_css

    def get_js(self):
        if not self.js:
            return tuple()
        t = typemap['js']
        return tuple(
            self.app.datafile(name, parent, t) for (name, parent) in self.js,
        )

    def get_inline_js(self):
        return self.inline_js

    def get_body_vars(self):
        return {}
