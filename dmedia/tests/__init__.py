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
Unit tests for `dmedia` package.
"""

from unittest import TestCase
from os import path

from desktopcouch.application.platform import find_port
from desktopcouch.application.local_files import get_oauth_tokens

import dmedia

datadir = path.join(path.dirname(path.abspath(__file__)), 'data')
sample_mov = path.join(datadir, 'MVI_5751.MOV')
sample_thm = path.join(datadir, 'MVI_5751.THM')

assert path.isdir(datadir)
assert path.isfile(sample_mov)
assert path.isfile(sample_thm)


class test_functions(TestCase):

    def test_get_env(self):
        # FIXME: Somehow this test is making gnomekeyring and
        # ~/.config/desktop-couch/desktop-couchdb.ini contain differnt values
        return
        f = dmedia.get_env
        port = find_port()
        url = 'http://localhost:%d/' % port
        oauth = get_oauth_tokens()

        self.assertEqual(
            f(),
            {'port': port, 'url': url, 'oauth': oauth, 'dbname': None}
        )
        self.assertEqual(
            f(dbname=None),
            {'port': port, 'url': url, 'oauth': oauth, 'dbname': None}
        )
        self.assertEqual(
            f(dbname='dmedia'),
            {'port': port, 'url': url, 'oauth': oauth, 'dbname': 'dmedia'}
        )
        self.assertEqual(
            f(dbname='dmedia_test'),
            {'port': port, 'url': url, 'oauth': oauth, 'dbname': 'dmedia_test'}
        )
