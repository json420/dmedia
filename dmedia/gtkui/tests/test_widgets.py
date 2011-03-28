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
Unit tests for `dmedia.gtkui.widgets` module.
"""

from unittest import TestCase

from oauth.oauth import OAuthConsumer, OAuthToken

from dmedia.schema import random_id
from dmedia.gtkui import widgets


# Test oauth tokens - don't relpace with tokens you actually use!
# Also, don't actually use these!
tokens = {
    'consumer_key': 'cVMqVGDNkC',
    'consumer_secret': 'lhpRuaFaeI',
    'token': 'NBGPaRrdXK',
    'token_secret': 'SGtppOobin'
}


class DummyRequest(object):
    def __init__(self, uri):
        self._uri = uri

    def get_uri(self):
        return self._uri


class DummyPolicy(object):
    def __init__(self):
        self._calls = []

    def ignore(self):
        self._calls.append('ignore')

    def use(self):
        self._calls.append('use')

    def download(self):
        self._calls.append('download')


class SignalCollector(object):
    def __init__(self, couchview):
        self._couchview = couchview
        couchview.connect('play', self.on_play)
        couchview.connect('open', self.on_open)
        self._sigs = []

    def on_play(self, cv, *args):
        assert cv is self._couchview
        self._sigs.append(('play',) + args)

    def on_open(self, cv, *args):
        assert cv is self._couchview
        self._sigs.append(('open',) + args)


class TestCouchView(TestCase):
    klass = widgets.CouchView

    def test_init(self):
        url = 'http://localhost:40705/dmedia/'
        netloc = 'localhost:40705'

        # Test with no oauth tokens provided:
        inst = self.klass(url)
        self.assertEqual(inst._couch_url, url)
        self.assertEqual(inst._couch_netloc, netloc)
        self.assertFalse(inst._oauth)
        self.assertFalse(hasattr(inst, '_consumer'))
        self.assertFalse(hasattr(inst, '_token'))

        # Test with oauth tokens:
        inst = self.klass(url, oauth_tokens=tokens)
        self.assertEqual(inst._couch_url, url)
        self.assertEqual(inst._couch_netloc, netloc)
        self.assertTrue(inst._oauth)
        self.assertIsInstance(inst._consumer, OAuthConsumer)
        self.assertIsInstance(inst._token, OAuthToken)

    def test_on_nav_policy_decision(self):
        # Method signature:
        # CouchView_on_nav_policy_decision(view, frame, request, nav, policy)

        url = 'http://localhost:40705/dmedia/'
        inst = self.klass(url)
        s = SignalCollector(inst)
        p = DummyPolicy()

        # Test a requset to desktopcouch
        r = DummyRequest('http://localhost:40705/foo/bar/baz')
        self.assertFalse(
            inst._on_nav_policy_decision(None, None, r, None, p)
        )
        self.assertEqual(p._calls, [])
        self.assertEqual(s._sigs, [])

        # Test a play:foo URI
        play = 'play:' + random_id() + '?start=17&end=69'
        r = DummyRequest(play)
        self.assertTrue(
            inst._on_nav_policy_decision(None, None, r, None, p)
        )
        self.assertEqual(p._calls, ['ignore'])
        self.assertEqual(s._sigs, [('play', play)])

        # Test opening an external URL
        lp = 'https://launchpad.net/dmedia'
        r = DummyRequest(lp)
        self.assertTrue(
            inst._on_nav_policy_decision(None, None, r, None, p)
        )
        self.assertEqual(p._calls, ['ignore', 'ignore'])
        self.assertEqual(s._sigs, [('play', play), ('open', lp)])

        # Test a URI that will just be ignored, not emit a signal
        nope = 'ftp://example.com'
        r = DummyRequest(nope)
        self.assertTrue(
            inst._on_nav_policy_decision(None, None, r, None, p)
        )
        self.assertEqual(p._calls, ['ignore', 'ignore', 'ignore'])
        self.assertEqual(s._sigs, [('play', play), ('open', lp)])
