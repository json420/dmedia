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
from .helpers import sample_mov, TempDir


class test_TranscodBin(TestCase):
    klass = transcoder.TranscodeBin

    def test_init(self):
        d = {
            'enc': 'vorbisenc',
            'props': {
                'quality': 0.5,
            },
        }
        inst = self.klass(d)
        self.assertTrue(inst._d is d)

        self.assertTrue(inst._q1.get_parent() is inst)
        self.assertTrue(isinstance(inst._q1, gst.Element))
        self.assertEqual(inst._q1.get_factory().get_name(), 'queue')

        self.assertTrue(inst._enc.get_parent() is inst)
        self.assertTrue(isinstance(inst._enc, gst.Element))
        self.assertEqual(inst._enc.get_factory().get_name(), 'vorbisenc')
        self.assertEqual(inst._enc.get_property('quality'), 0.5)

        self.assertTrue(inst._q2.get_parent() is inst)
        self.assertTrue(isinstance(inst._q2, gst.Element))
        self.assertEqual(inst._q2.get_factory().get_name(), 'queue')

        d = {'enc': 'vorbisenc'}
        inst = self.klass(d)
        self.assertTrue(inst._d is d)

        self.assertTrue(inst._q1.get_parent() is inst)
        self.assertTrue(isinstance(inst._q1, gst.Element))
        self.assertEqual(inst._q1.get_factory().get_name(), 'queue')

        self.assertTrue(inst._enc.get_parent() is inst)
        self.assertTrue(isinstance(inst._enc, gst.Element))
        self.assertEqual(inst._enc.get_factory().get_name(), 'vorbisenc')
        self.assertNotEqual(inst._enc.get_property('quality'), 0.5)

        self.assertTrue(inst._q2.get_parent() is inst)
        self.assertTrue(isinstance(inst._q2, gst.Element))
        self.assertEqual(inst._q2.get_factory().get_name(), 'queue')

    def test_repr(self):
        d = {
            'enc': 'vorbisenc',
            'props': {
                'quality': 0.5,
            },
        }

        inst = self.klass(d)
        self.assertEqual(
            repr(inst),
            'TranscodeBin(%r)' % (d,)
        )

        class FooBar(self.klass):
            pass
        inst = FooBar(d)
        self.assertEqual(
            repr(inst),
            'FooBar(%r)' % (d,)
        )

    def test_make(self):
        d = {'enc': 'vorbisenc'}
        inst = self.klass(d)

        enc = inst._make('theoraenc')
        self.assertTrue(enc.get_parent() is inst)
        self.assertTrue(isinstance(enc, gst.Element))
        self.assertEqual(enc.get_factory().get_name(), 'theoraenc')
        self.assertEqual(enc.get_property('quality'), 48)
        self.assertEqual(enc.get_property('keyframe-force'), 64)

        enc = inst._make('theoraenc', {'quality': 50, 'keyframe-force': 32})
        self.assertTrue(enc.get_parent() is inst)
        self.assertTrue(isinstance(enc, gst.Element))
        self.assertEqual(enc.get_factory().get_name(), 'theoraenc')
        self.assertEqual(enc.get_property('quality'), 50)
        self.assertEqual(enc.get_property('keyframe-force'), 32)


class test_AudioTranscoder(TestCase):
    klass = transcoder.AudioTranscoder

    def test_init(self):
        d = {
            'enc': 'vorbisenc',
            'props': {
                'quality': 0.5,
            },
        }
        inst = self.klass(d)
        self.assertTrue(isinstance(inst._enc, gst.Element))
        self.assertEqual(inst._enc.get_factory().get_name(), 'vorbisenc')
        self.assertEqual(inst._enc.get_property('quality'), 0.5)

        d = {
            'enc': 'vorbisenc',
            'caps': 'audio/x-raw-float, rate=44100',
            'props': {
                'quality': 0.25,
            },
        }
        inst = self.klass(d)
        self.assertTrue(isinstance(inst._enc, gst.Element))
        self.assertEqual(inst._enc.get_factory().get_name(), 'vorbisenc')
        self.assertEqual(inst._enc.get_property('quality'), 0.25)


class test_VideoTranscoder(TestCase):
    klass = transcoder.VideoTranscoder

    def test_init(self):
        d = {
            'enc': 'theoraenc',
            'props': {
                'quality': 50,
                'keyframe-force': 32,
            },
        }
        inst = self.klass(d)
        self.assertTrue(isinstance(inst._enc, gst.Element))
        self.assertEqual(inst._enc.get_factory().get_name(), 'theoraenc')
        self.assertEqual(inst._enc.get_property('quality'), 50)

        d = {
            'enc': 'theoraenc',
            'caps': 'video/x-raw-yuv, width=800, height=450',
            'props': {
                'quality': 50,
                'keyframe-force': 32,
            },
        }
        inst = self.klass(d)
        self.assertTrue(isinstance(inst._enc, gst.Element))
        self.assertEqual(inst._enc.get_factory().get_name(), 'theoraenc')
        self.assertEqual(inst._enc.get_property('quality'), 50)


class test_Transcoder(TestCase):
    klass = transcoder.Transcoder

    def test_init(self):
        tmp = TempDir()
        src = tmp.copy(sample_mov, 'src.mov')
        dst = tmp.join('dst.mov')
        d = {'mux': 'oggmux'}

        inst = self.klass(src, dst, d)
        self.assertTrue(inst.d is d)

        self.assertTrue(isinstance(inst.src, gst.Element))
        self.assertTrue(inst.src.get_parent() is inst.pipeline)
        self.assertEqual(inst.src.get_factory().get_name(), 'filesrc')
        self.assertEqual(inst.src.get_property('location'), src)

        self.assertTrue(isinstance(inst.dec, gst.Element))
        self.assertTrue(inst.dec.get_parent() is inst.pipeline)
        self.assertEqual(inst.dec.get_factory().get_name(), 'decodebin2')

        self.assertTrue(isinstance(inst.mux, gst.Element))
        self.assertTrue(inst.mux.get_parent() is inst.pipeline)
        self.assertEqual(inst.mux.get_factory().get_name(), 'oggmux')

        self.assertTrue(isinstance(inst.sink, gst.Element))
        self.assertTrue(inst.sink.get_parent() is inst.pipeline)
        self.assertEqual(inst.sink.get_factory().get_name(), 'filesink')
        self.assertEqual(inst.sink.get_property('location'), dst)
