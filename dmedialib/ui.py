# Authors:
#   Jason Gerard DeRose <jderose@jasonderose.org>
#
# dmedia: distributed media library
# Copyright (C) 2010 Jason Gerard DeRose <jderose@jasonderose.org>
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# `dmedia` is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License along
# with `dmedia`.  If not, see <http://www.gnu.org/licenses/>.

"""
Web UI for dmedia browser.
"""

from os import path
from genshi.template import MarkupTemplate
from . import datadir

CONTENT_TYPE = 'application/xhtml+xml; charset=utf-8'

DEFAULT_KW = (
    ('lang', 'en'),
    ('title', None),
    ('content_type', CONTENT_TYPE),
    ('inline_css', None),
    ('inline_js', None),
)


def datafile(name):
    """
    Return absolute path of datafile named *name*.
    """
    return path.join(datadir, name)


def load_datafile(name):
    return open(datafile(name), 'r').read()


def load_template(name):
    return MarkupTemplate(load_datafile(name), filename=datafile(name))


def render_template(template, **kw):
    kw2 = dict(DEFAULT_KW)
    kw2.update(kw)
    return template.generate(**kw2).render('xhtml', doctype='xhtml11')


class WSGIApp(object):
    scripts = ('dmedia.js',)

    def __init__(self):
        self.template = load_template('browser.xml')
        self.js = '\n\n'.join(load_datafile(n) for n in self.scripts)

    def __call__(self, environ, start_response):
        s = render_template(self.template, inline_js=self.js)
        response_headers = [
            ('Content-Type', CONTENT_TYPE),
            ('Content-Length', str(len(s))),
        ]
        start_response('200 OK', response_headers)
        return [s]

    def render(self):
        return render_template(self.template, inline_js=self.js)
