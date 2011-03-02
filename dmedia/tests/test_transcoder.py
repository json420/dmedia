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
from dmedia.constants import TYPE_ERROR
from dmedia.filestore import FileStore
from .helpers import sample_mov, mov_hash, TempDir, raises


class test_functions(TestCase):
    def test_caps_string(self):
        f = transcoder.caps_string
        self.assertEqual(
            f('audio/x-raw-float', {}),
            'audio/x-raw-float'
        )
        self.assertEqual(
            f('audio/x-raw-float', {'rate': 44100}),
            'audio/x-raw-float, rate=44100'
        )
        self.assertEqual(
            f('audio/x-raw-float', {'rate': 44100, 'channels': 1}),
            'audio/x-raw-float, channels=1, rate=44100'
        )


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
            'caps': {'rate': 44100},
            'props': {'quality': 0.25},
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
            'caps': {'width': 800, 'height': 450},
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

    def setUp(self):
        self.tmp = TempDir()
        self.fs = FileStore(self.tmp.path)
        self.fs.import_file(open(sample_mov, 'rb'), 'mov')

    def test_init(self):
        job = {
            'src': {'id': mov_hash, 'ext': 'mov'},
            'mux': 'oggmux',
            'ext': 'ogv',
        }

        e = raises(TypeError, self.klass, 17, self.fs)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('job', dict, int, 17)
        )
        e = raises(TypeError, self.klass, job, 18)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('fs', FileStore, int, 18)
        )

        inst = self.klass(job, self.fs)
        self.assertTrue(inst.job is job)
        self.assertTrue(inst.fs is self.fs)

        self.assertTrue(isinstance(inst.src_fp, file))
        self.assertEqual(inst.src_fp.mode, 'rb')
        self.assertEqual(
            inst.src_fp.name,
            self.tmp.join(mov_hash[:2], mov_hash[2:] + '.mov')
        )

        self.assertTrue(isinstance(inst.dst_fp, file))
        self.assertEqual(inst.dst_fp.mode, 'r+b')
        self.assertTrue(
            inst.dst_fp.name.startswith(self.tmp.join('writes'))
        )
        self.assertTrue(inst.dst_fp.name.endswith('.ogv'))

        self.assertTrue(isinstance(inst.src, gst.Element))
        self.assertTrue(inst.src.get_parent() is inst.pipeline)
        self.assertEqual(inst.src.get_factory().get_name(), 'fdsrc')
        self.assertEqual(inst.src.get_property('fd'), inst.src_fp.fileno())

        self.assertTrue(isinstance(inst.dec, gst.Element))
        self.assertTrue(inst.dec.get_parent() is inst.pipeline)
        self.assertEqual(inst.dec.get_factory().get_name(), 'decodebin2')

        self.assertTrue(isinstance(inst.mux, gst.Element))
        self.assertTrue(inst.mux.get_parent() is inst.pipeline)
        self.assertEqual(inst.mux.get_factory().get_name(), 'oggmux')

        self.assertTrue(isinstance(inst.sink, gst.Element))
        self.assertTrue(inst.sink.get_parent() is inst.pipeline)
        self.assertEqual(inst.sink.get_factory().get_name(), 'fdsink')
        self.assertEqual(inst.sink.get_property('fd'), inst.dst_fp.fileno())

    def test_theora450(self):
        job = {
            'src': {'id': mov_hash, 'ext': 'mov'},
            'mux': 'oggmux',
            'video': {
                'enc': 'theoraenc',
                'caps': {'width': 800, 'height': 450},
            },
        }
        inst = self.klass(job, self.fs)
        inst.run()

    def test_flac(self):
        job = {
            'src': {'id': mov_hash, 'ext': 'mov'},
            'mux': 'oggmux',
            'audio': {
                'enc': 'flacenc',
                'caps': {'rate': 44100},
            },
        }
        inst = self.klass(job, self.fs)
        inst.run()

    def test_theora360_vorbis(self):
        job = {
            'src': {'id': mov_hash, 'ext': 'mov'},
            'mux': 'oggmux',
            'video': {
                'enc': 'theoraenc',
                'props': {'quality': 40},
                'caps': {'width': 800, 'height': 450},
            },
            'audio': {
                'enc': 'vorbisenc',
                'props': {'quality': 0.4},
                'caps': {'rate': 44100},
            },
        }
        inst = self.klass(job, self.fs)
        inst.run()
