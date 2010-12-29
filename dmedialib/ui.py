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
import mimetypes
from genshi.template import MarkupTemplate
from . import datadir

mimetypes.init()

CONTENT_TYPE = 'application/xhtml+xml; charset=utf-8'

DEFAULT_KW = (
    ('lang', 'en'),
    ('title', None),
    ('content_type', CONTENT_TYPE),
    ('links_css', tuple()),
    ('inline_css', None),
    ('links_js', tuple()),
    ('inline_js', None),
    ('body', None),
)


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


def inline_data(names):
    return '\n\n'.join(
        inline_datafile(name) for name in names
    )


def encode_datafile(name):
    """
    Read datafile *name* and return base64-encoded.
    """
    return b64encode(load_datafile(name))


def iter_datafiles():
    for name in sorted(os.listdir(datadir)):
        if name.startswith('.') or name.endswith('~'):
            continue
        if name.endswith('.xml'):
            continue
        if not path.isfile(path.join(datadir, name)):
            continue
        ext = path.splitext(name)[1]
        yield (name, mimetypes.types_map.get(ext))


def create_app():
    att = dict(
        (name, {'content_type': mime, 'data': encode_datafile(name)})
        for (name, mime) in iter_datafiles()
    )
    att['browser.xhtml'] = {
        'content_type': CONTENT_TYPE,
        'data': encode_template(load_template('browser.xml')),
    }
    return {
        '_id': 'app',
        '_attachments': att,
    }


def load_template(name):
    return MarkupTemplate(load_datafile(name), filename=datafile(name))


def render_template(template, **kw):
    kw2 = dict(DEFAULT_KW)
    kw2.update(kw)
    return template.generate(**kw2).render('xhtml', doctype='xhtml11')


def encode_template(template, **kw):
    return b64encode(render_template(template, **kw))


class Page(object):
    toplevel = 'toplevel.xml'
    body = None

    inline_css_files = tuple()
    inline_js_files = tuple()
    inline_css = ''
    inline_js = ''

    def __init__(self):
        self.toplevel_t = load_template(self.toplevel)
        self.body_t = (load_template(self.body) if self.body else None)
        if self.inline_css_files:
            self.inline_css = inline_data(self.inline_css_files)
        if self.inline_js_files:
            self.inline_js = inline_data(self.inline_js_files)

    def render(self):
        pass




class WSGIApp(object):
    scripts = ('mootools.js', 'dmedia.js')
    styles = ('dmedia.css',)

    def __init__(self):
        self.template = load_template('browser.xml')
        self.js = '\n\n'.join(load_datafile(n) for n in self.scripts)
        self.css = '\n\n'.join(load_datafile(n) for n in self.styles)
        print self.css

    def __call__(self, environ, start_response):
        s = self.render()
        response_headers = [
            ('Content-Type', CONTENT_TYPE),
            ('Content-Length', str(len(s))),
        ]
        start_response('200 OK', response_headers)
        return [s]

    def render(self):
        return render_template(self.template,
            inline_js=self.js,
            inline_css=self.css,
        )
