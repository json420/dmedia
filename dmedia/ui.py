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
from urlparse import urlparse, parse_qsl
from gi.repository import WebKit
from oauth import oauth
from desktopcouch.local_files import get_oauth_tokens
from . import datadir


def render_var(name, obj):
    """
    Render *obj* to JavaScript variable *name*.

    For example:

    >>> render_var('dmedia', dict(hello='world', foo='bar'))
    'var dmedia = {"foo": "bar", "hello": "world"};'

    Also works for object attribute assignment:

    >>> render_var('dmedia.data', dict(hello='world', foo='bar'))
    'dmedia.data = {"foo": "bar", "hello": "world"};'

    """
    format = ('%s = %s;' if '.' in name else 'var %s = %s;')
    return format % (name, json.dumps(obj, sort_keys=True))


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


class CouchView(WebKit.WebView):
    """
    Transparently sign desktopcouch requests with OAuth.

    desktopcouch uses OAuth to authenticate HTTP requests to CouchDB.  Well,
    technically it can also use basic auth, but if you do this, Stuart Langridge
    will be very cross with you!

    This class wraps a ``gi.repository.WebKit.WebView`` so that you can have a
    single web app that:

        1. Can run in a browser and talk to a remote CouchDB over HTTPS with
           basic auth

        2. Can also run in embedded WebKit and talk to the local desktopcouch
           over HTTP with OAuth

    Being able to do this sort of thing transparently is a big reason why dmedia
    and Novacut are designed the way they are.

    For some background, see:

        https://bugs.launchpad.net/dmedia/+bug/677697

        http://oauth.net/

    Special thanks to Stuart Langridge for the example code that helped get this
    working.
    """
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
        # FIXME: For the Novacut player, we need a way for external links (say
        # to artist's homepage) to open in a regular browser using `xdg-open`.
        uri = request.get_uri()
        c = urlparse(uri)
        query = dict(parse_qsl(c.query))
        # Handle bloody CouchDB having foo.html?dbname URLs, that is
        # a querystring which isn't of the form foo=bar
        if c.query and not query:
            query = {c.query: ''}
        req = oauth.OAuthRequest.from_consumer_and_token(
            self._consumer,
            self._token,
            http_method=request.props.message.props.method,
            http_url=uri,
            parameters=query,
        )
        req.sign_request(
            oauth.OAuthSignatureMethod_HMAC_SHA1(),
            self._consumer,
            self._token
        )
        request.props.message.props.request_headers.append(
            'Authorization', req.to_header()['Authorization']
        )
