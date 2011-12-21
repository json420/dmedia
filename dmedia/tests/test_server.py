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

from filestore import DIGEST_B32LEN, DIGEST_BYTES
from microfiber import random_id

from dmedia import server, client


class StartResponse:
    def __init__(self):
        self.__called = False

    def __call__(self, status, headers):
        assert not self.__callled
        self.__called = True
        self.status = status
        self.headers = headers


class TestFunctions(TestCase):
    def test_get_slice(self):
        # Test all the valid types of requests:
        _id = random_id(DIGEST_BYTES)
        self.assertEqual(
            server.get_slice({'PATH_INFO': '/{}'.format(_id)}),
            (_id, 0, None)
        )

        _id = random_id(DIGEST_BYTES)
        self.assertEqual(
            server.get_slice({'PATH_INFO': '/{}/0'.format(_id)}),
            (_id, 0, None)
        )

        _id = random_id(DIGEST_BYTES)
        self.assertEqual(
            server.get_slice({'PATH_INFO': '/{}/17'.format(_id)}),
            (_id, 17, None)
        )

        _id = random_id(DIGEST_BYTES)
        self.assertEqual(
            server.get_slice({'PATH_INFO': '/{}/17/21'.format(_id)}),
            (_id, 17, 21)
        )

        _id = random_id(DIGEST_BYTES)
        self.assertEqual(
            server.get_slice({'PATH_INFO': '/{}/0/1'.format(_id)}),
            (_id, 0, 1)
        )

        # Too many slashes
        with self.assertRaises(server.BadRequest) as cm:
            server.get_slice({'PATH_INFO': '/file-id/start/stop/other'})
        self.assertEqual(cm.exception.body, b'too many slashes in request path')

        with self.assertRaises(server.BadRequest) as cm:
            server.get_slice({'PATH_INFO': 'file-id/start/stop/'})
        self.assertEqual(cm.exception.body, b'too many slashes in request path')

        with self.assertRaises(server.BadRequest) as cm:
            server.get_slice({'PATH_INFO': '/file-id///'})
        self.assertEqual(cm.exception.body, b'too many slashes in request path')

        # Bad ID
        attack = 'CCCCCCCCCCCCCCCCCCCCCCCCCCC\..\..\..\.ssh\id_rsa'
        self.assertEqual(len(attack), DIGEST_B32LEN)
        with self.assertRaises(server.BadRequest) as cm:
            server.get_slice({'PATH_INFO': attack})
        self.assertEqual(cm.exception.body, b'badly formed dmedia ID')

        short = random_id(DIGEST_BYTES - 5)
        with self.assertRaises(server.BadRequest) as cm:
            server.get_slice({'PATH_INFO': short})
        self.assertEqual(cm.exception.body, b'badly formed dmedia ID')

        long = random_id(DIGEST_BYTES + 5)
        with self.assertRaises(server.BadRequest) as cm:
            server.get_slice({'PATH_INFO': long})
        self.assertEqual(cm.exception.body, b'badly formed dmedia ID')

        lower = random_id(DIGEST_BYTES).lower()
        with self.assertRaises(server.BadRequest) as cm:
            server.get_slice({'PATH_INFO': lower})
        self.assertEqual(cm.exception.body, b'badly formed dmedia ID')

        # start not integer
        bad = '/{}/17.9'.format(random_id(DIGEST_BYTES))
        with self.assertRaises(server.BadRequest) as cm:
            server.get_slice({'PATH_INFO': bad})
        self.assertEqual(cm.exception.body, b'start is not a valid integer')

        bad = '/{}/00ff'.format(random_id(DIGEST_BYTES))
        with self.assertRaises(server.BadRequest) as cm:
            server.get_slice({'PATH_INFO': bad})
        self.assertEqual(cm.exception.body, b'start is not a valid integer')

        bad = '/{}/foo'.format(random_id(DIGEST_BYTES))
        with self.assertRaises(server.BadRequest) as cm:
            server.get_slice({'PATH_INFO': bad})
        self.assertEqual(cm.exception.body, b'start is not a valid integer')

        bad = '/{}/17.9/333'.format(random_id(DIGEST_BYTES))
        with self.assertRaises(server.BadRequest) as cm:
            server.get_slice({'PATH_INFO': bad})
        self.assertEqual(cm.exception.body, b'start is not a valid integer')

        bad = '/{}/00ff/333'.format(random_id(DIGEST_BYTES))
        with self.assertRaises(server.BadRequest) as cm:
            server.get_slice({'PATH_INFO': bad})
        self.assertEqual(cm.exception.body, b'start is not a valid integer')

        bad = '/{}/foo/333'.format(random_id(DIGEST_BYTES))
        with self.assertRaises(server.BadRequest) as cm:
            server.get_slice({'PATH_INFO': bad})
        self.assertEqual(cm.exception.body, b'start is not a valid integer')

        # stop not integer
        bad = '/{}/18/21.2'.format(random_id(DIGEST_BYTES))
        with self.assertRaises(server.BadRequest) as cm:
            server.get_slice({'PATH_INFO': bad})
        self.assertEqual(cm.exception.body, b'stop is not a valid integer')

        bad = '/{}/18/00ff'.format(random_id(DIGEST_BYTES))
        with self.assertRaises(server.BadRequest) as cm:
            server.get_slice({'PATH_INFO': bad})
        self.assertEqual(cm.exception.body, b'stop is not a valid integer')

        bad = '/{}/18/foo'.format(random_id(DIGEST_BYTES))
        with self.assertRaises(server.BadRequest) as cm:
            server.get_slice({'PATH_INFO': bad})
        self.assertEqual(cm.exception.body, b'stop is not a valid integer')

        # start < 0
        bad = '/{}/-1'.format(random_id(DIGEST_BYTES))
        with self.assertRaises(server.BadRequest) as cm:
            server.get_slice({'PATH_INFO': bad})
        self.assertEqual(cm.exception.body, b'start cannot be less than zero')

        bad = '/{}/-1/18'.format(random_id(DIGEST_BYTES))
        with self.assertRaises(server.BadRequest) as cm:
            server.get_slice({'PATH_INFO': bad})
        self.assertEqual(cm.exception.body, b'start cannot be less than zero')

        # start >= stop
        bad = '/{}/18/17'.format(random_id(DIGEST_BYTES))
        with self.assertRaises(server.BadRequest) as cm:
            server.get_slice({'PATH_INFO': bad})
        self.assertEqual(cm.exception.body, b'start must be less than stop')

        bad = '/{}/17/17'.format(random_id(DIGEST_BYTES))
        with self.assertRaises(server.BadRequest) as cm:
            server.get_slice({'PATH_INFO': bad})
        self.assertEqual(cm.exception.body, b'start must be less than stop')

    def test_range_to_slice(self):
        with self.assertRaises(server.BadRangeRequest) as cm:
            (start, stop) = server.range_to_slice('goats=0-500')
        self.assertEqual(cm.exception.body, b'bad range units')

        with self.assertRaises(server.BadRangeRequest) as cm:
            (start, stop) = server.range_to_slice('bytes=-500-999')
        self.assertEqual(cm.exception.body, b'range -start is not an integer')

        with self.assertRaises(server.BadRangeRequest) as cm:
            (start, stop) = server.range_to_slice('bytes=-foo')
        self.assertEqual(cm.exception.body, b'range -start is not an integer')

        with self.assertRaises(server.BadRangeRequest) as cm:
            (start, stop) = server.range_to_slice('bytes=500')
        self.assertEqual(cm.exception.body, b'not formatted as bytes=start-end')

        with self.assertRaises(server.BadRangeRequest) as cm:
            (start, stop) = server.range_to_slice('bytes=foo-999')
        self.assertEqual(cm.exception.body, b'range start is not an integer')

        with self.assertRaises(server.BadRangeRequest) as cm:
            (start, stop) = server.range_to_slice('bytes=500-bar')
        self.assertEqual(cm.exception.body, b'range end is not an integer')

        with self.assertRaises(server.BadRangeRequest) as cm:
            (start, stop) = server.range_to_slice('bytes=500-499')
        self.assertEqual(
            cm.exception.body,
            b'range end must be less than or equal to start'
        )

        self.assertEqual(
            server.range_to_slice('bytes=0-0'), (0, 1)
        )
        self.assertEqual(
            server.range_to_slice('bytes=0-499'), (0, 500)
        )
        self.assertEqual(
            server.range_to_slice('bytes=500-999'), (500, 1000)
        )
        self.assertEqual(
            server.range_to_slice('bytes=9500-9999'), (9500, 10000)
        )
        self.assertEqual(
            server.range_to_slice('bytes=9500-'), (9500, None)
        )
        self.assertEqual(
            server.range_to_slice('bytes=-500'), (-500, None)
        )

        # Test the round-trip with client.bytes_range
        slices = [
            (0, 1),
            (0, 500),
            (500, 1000),
            (9500, 10000),
            (-500, None),
            (9500, None),
        ]
        for (start, stop) in slices:
            self.assertEqual(
                server.range_to_slice(client.bytes_range(start, stop)),
                (start, stop)
            )


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

