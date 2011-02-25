# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#
# dmedia: distributed media library
# Copyright (C) 2011 Jason Gerard DeRose <jderose@novacut.com>
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
Unit tests for `dmedia.transcoder` module.
"""

from unittest import TestCase

import gst

from dmedia import transcoder


class test_functions(TestCase):
    def test_make_encoder(self):
        f = transcoder.make_encoder

        d = {
            'enc': 'vorbisenc',
            'props': {
                'quality': 0.5,
            },
        }
        enc = f(d)
        self.assertTrue(isinstance(enc, gst.Element))
        self.assertEqual(enc.get_factory().get_name(), 'vorbisenc')
        self.assertEqual(enc.get_property('quality'), 0.5)

        d = {
            'enc': 'theoraenc',
            'props': {},
        }
        enc = f(d)
        self.assertTrue(isinstance(enc, gst.Element))
        self.assertEqual(enc.get_factory().get_name(), 'theoraenc')
        self.assertEqual(enc.get_property('quality'), 48)
        self.assertEqual(enc.get_property('keyframe-force'), 64)

        d = {
            'enc': 'theoraenc',
            'props': {
                'quality': 50,
                'keyframe-force': 32,
            },
        }
        enc = f(d)
        self.assertTrue(isinstance(enc, gst.Element))
        self.assertEqual(enc.get_factory().get_name(), 'theoraenc')
        self.assertEqual(enc.get_property('quality'), 50)
        self.assertEqual(enc.get_property('keyframe-force'), 32)
