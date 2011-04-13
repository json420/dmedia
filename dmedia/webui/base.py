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
Base class for Pages and Apps.
"""

from base64 import b64encode

from util import datafile, load_data, load_template

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
    pages = tuple()

    def __init__(self):
        self._data = {}
        self._datafiles = {}
        self._templates = {}
        self.name = self.__class__.__name__.lower()

    def datafile(self, name, parent=None, mime=None):
        filename = datafile(name, parent)
        content_type = get_mime(name, mime)
        if name in self._data:
            assert self._data[name]['content_type'] == content_type
            assert self._datafiles[name] == filename
            return name
        self._data[name] = {
            'content_type': content_type,
            'data': load_data(filename),
        }
        self._datafiles[name] = filename
        return name

    def template(self, name, parent=None):
        filename = datafile(name, parent)
        try:
            return self._templates[filename]
        except KeyError:
            t = load_template(filename)
            self._templates[filename] = t
            return t

    def render(self):
        for klass in self.pages:
            page = klass(self)
            for asset in page.assets:
                self.datafile(*asset)
            self._data[page.name] = {
                'content_type': page.content_type,
                'data': page.render(),
            }
        return self._data

    def b64render(self):
        self.render()
        return dict(
            (
                name,
                {
                    'content_type': d['content_type'],
                    'data': b64encode(d['data']),
                }
            )
            for (name, d) in self._data.items()
        )

    def get_doc(self):
        return {
            '_id': self.name,
            '_attachments': self.b64render(),
        }



class Page(object):
    def __init__(self, app):
        self.app = app
        self.name = self.__class__.__name__.lower()

    # You probably dont want to change these:
    content_type = 'text/html; charset=UTF-8'
    serializer = 'xhtml'
    doctype = 'html5'
    top = ('top.xml', None)  # Top level template

    # Definitely do want to change these:
    title = 'To change, override `Page.title`'
    body = ('placeholder.xml', None)  # The <body>...</body> template

    # CSS:
    css = (
        ('base.css', None),
    )
    inline_css = None

    # JavaScript:
    js = (
        ('couch.js', None),
    )
    inline_js = None

    # Other assets:
    assets = tuple()

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
