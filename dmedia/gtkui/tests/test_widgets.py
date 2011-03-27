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

from dmedia.gtkui import widgets


# Test oauth tokens - don't relpace with tokens you actually use!
# Also, don't actually use these!
tokens = {
    'consumer_key': 'cVMqVGDNkC',
    'consumer_secret': 'lhpRuaFaeI',
    'token': 'NBGPaRrdXK',
    'token_secret': 'SGtppOobin'
}


class TestCouchView(TestCase):
    klass = widgets.CouchView

    def test_init(self):
        # Test with no oauth tokens provided:
        inst = self.klass()
        self.assertFalse(inst._oauth)
        self.assertFalse(hasattr(inst, '_consumer'))
        self.assertFalse(hasattr(inst, '_token'))

        # Test with oauth tokens:
        inst = self.klass(oauth_tokens=tokens)
        self.assertTrue(inst._oauth)
        self.assertIsInstance(inst._consumer, OAuthConsumer)
        self.assertIsInstance(inst._token, OAuthToken)
