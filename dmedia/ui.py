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
from urlparse import urlparse, parse_qs
import webkit
from oauth import oauth
from desktopcouch.local_files import get_oauth_tokens
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


class CouchView(webkit.WebView):
    def __init__(self):
        super(CouchView, self).__init__()
        self.connect('resource-request-starting', self._on_nav)
        oauth_data = get_oauth_tokens()
        self._consumer = oauth.OAuthConsumer(
            oauth_data['consumer_key'],
            oauth_data['consumer_secret']
        )
        self._token = oauth.OAuthToken(
            oauth_data['token'],
            oauth_data['token_secret']
        )

    def _on_nav(self, view, frame, resource, request, response):
        # This seems to be a good way to filter out data: URIs
        if request.props.message is None:
            return
        uri = request.get_uri()
        print uri
        c = urlparse(uri)
        req = oauth.OAuthRequest.from_consumer_and_token(
            self._consumer,
            self._token,
            http_method=request.props.message.props.method,
            http_url=uri,
            parameters=parse_qs(c.query)
        )
        req.sign_request(
            oauth.OAuthSignatureMethod_HMAC_SHA1(),
            self._consumer,
            self._token
        )
        request.set_uri(req.to_url())
        return
        # FIXME: Apparrently we can't actually modify the headers from Python
        request.props.message.props.request_headers.append(
            'Authorization', req.to_header()['Authorization']
        )
