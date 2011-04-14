# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#   David Green <david4dev@gmail.com>
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
Custom dmedia GTK widgets, currently just `CouchView` and `BrowserMenu`.
"""

from urlparse import urlparse, parse_qsl

from oauth import oauth
from gi.repository import GObject, WebKit, Gtk

from .menu import MENU, ACTIONS


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

    __gsignals__ = {
        'play': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [GObject.TYPE_PYOBJECT]
        ),
        'open': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [GObject.TYPE_PYOBJECT]
        ),
    }

    def __init__(self, couch_url, oauth_tokens=None):
        super(CouchView, self).__init__()
        self._couch_url = couch_url
        self._couch_netloc = urlparse(couch_url).netloc
        self.connect('resource-request-starting', self._on_resource_request)
        self.connect('navigation-policy-decision-requested',
            self._on_nav_policy_decision
        )
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

    def _on_nav_policy_decision(self, view, frame, request, nav, policy):
        """
        Handle user trying to Navigate away from current page.

        Note that this will be called before `CouchView._on_resource_request()`.

        The *policy* arg is a ``WebPolicyDecision`` instance.  To handle the
        decision, call one of:

            * ``WebPolicyDecision.ignore()``
            * ``WebPolicyDecision.use()``
            * ``WebPolicyDecision.download()``

        And then return ``True``.

        Otherwise, return ``False`` or ``None`` to have the WebKit default
        behavior apply.
        """
        uri = request.get_uri()
        u = urlparse(uri)
        if u.netloc == self._couch_netloc and u.scheme in ('http', 'https'):
            return False
        if uri.startswith('play:'):
            self.emit('play', uri)
        elif u.netloc != self._couch_netloc and u.scheme in ('http', 'https'):
            self.emit('open', uri)
        policy.ignore()
        return True

    def _on_resource_request(self, view, frame, resource, request, response):
        """
        When appropriate, OAuth-sign the request prior to it being sent.

        This will only sign requests on the same host and port in the URL passed
        to `CouchView.__init__()`.
        """
        uri = request.get_uri()
        u = urlparse(uri)
        if u.scheme not in ('http', 'https'):  # Ignore data:foo requests
            return
        if u.netloc != self._couch_netloc:  # Dont sign requests to broader net!
            return
        if not self._oauth:  # OAuth info wasn't provided
            return
        query = dict(parse_qsl(u.query))
        # Handle bloody CouchDB having foo.html?dbname URLs, that is
        # a querystring which isn't of the form foo=bar
        if u.query and not query:
            query = {u.query: ''}
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


class BrowserMenu(Gtk.MenuBar):
    def __init__(self, menu=MENU, actions=ACTIONS):
        super(BrowserMenu, self).__init__()
        self.show()
        self.menu = menu
        self.actions = actions
        self.reset()

    def add_items_to_menu(self, menu, *items):
        for item in items:
            menu.append(item)

    def make_menu(self, menu):
        items = []
        for i in menu:
            item = Gtk.MenuItem()
            item.show()
            item.set_label(i["label"])
            if i["type"] == "menu":
                submenu = Gtk.Menu()
                submenu.show()
                self.add_items_to_menu(submenu, *self.make_menu(i["items"]))
                item.set_submenu(submenu)
            elif i["type"] == "action":
                item.connect("activate", self.actions[i["action"]])
            items.append(item)
        return items

    def reset(self):
        self.add_items_to_menu(self, *self.make_menu(self.menu))


