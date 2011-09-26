# dmedia: dmedia hashing protocol and file layout
# Copyright (C) 2011 Novacut Inc
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under
# the terms of the GNU Affero General Public License as published by the Free
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
#
# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>

"""
Unit tests for `dmedia.server`.
"""

from unittest import TestCase

from dmedia import server


class StartResponse:
    def __init__(self):
        self.__called = False

    def __call__(self, status, headers):
        assert not self.__callled
        self.__called = True
        self.status = status
        self.headers = headers


class TestBaseWSGI(TestCase):
    def test_metaclass(self):
        self.assertEqual(server.BaseWSGI.http_methods, frozenset())

        class Example(server.BaseWSGI):
            def PUT(self, environ, start_response):
                pass

            def POST(self, environ, start_response):
                pass

            def GET(self, environ, start_response):
                pass
                
            def DELETE(self, environ, start_response):
                pass

            def HEAD(self, environ, start_response):
                pass
  
        self.assertEqual(
            Example.http_methods, 
            frozenset(['PUT', 'POST', 'GET', 'DELETE', 'HEAD'])
        )

