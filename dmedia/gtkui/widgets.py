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
Custom dmedia GTK widgets, currently just `CouchView`.
"""

from urlparse import urlparse, parse_qsl

from oauth import oauth
from gi.repository import WebKit


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

    def __init__(self, oauth_tokens=None):
        super(CouchView, self).__init__()
        self.connect('resource-request-starting', self._on_nav)
        if oauth_tokens:
            self._oauth = True
            self._consumer = oauth.OAuthConsumer(
                oauth_tokens['consumer_key'],
                oauth_tokens['consumer_secret']
            )
            self._token = oauth.OAuthToken(
                oauth_tokens['token'],
                oauth_tokens['token_secret']
            )
        else:
            self._oauth = False

    def _on_nav(self, view, frame, resource, request, response):
        # This seems to be a good way to filter out data: URIs
        if request.props.message is None:
            return
        if not self._oauth:
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
