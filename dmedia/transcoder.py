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
GStreamer-based transcoder.
"""

import logging

from gi.repository import GObject, Gst

from .constants import TYPE_ERROR
from .filestore import FileStore


log = logging.getLogger()
Gst.init(None)


def caps_string(mime, caps):
    """
    Build a GStreamer caps string.

    For example:

    >>> caps_string('video/x-raw-yuv', {'width': 800, 'height': 450})
    'video/x-raw-yuv, height=450, width=800'
    """
    accum = [mime]
    for key in sorted(caps):
        accum.append('%s=%s' % (key, caps[key]))
    return ', '.join(accum)



class TranscodeBin(Gst.Bin):
    """
    Base class for `AudioTranscoder` and `VideoTranscoder`.
    """
    def __init__(self, d):
        super(TranscodeBin, self).__init__()
        self._d = d
        self._q1 = self._make('queue')
        self._enc = self._make(d['enc'], d.get('props'))
        self._q2 = self._make('queue')
        self._enc.link(self._q2)
        self.add_pad(
            Gst.GhostPad.new('sink', self._q1.get_pad('sink'))
        )
        self.add_pad(
            Gst.GhostPad.new('src', self._q2.get_pad('src'))
        )

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self._d)

    def _make(self, name, props=None):
        """
        Create gst element, set properties, and add to this bin.
        """
        element = Gst.ElementFactory.make(name, None)
        if props:
            for (key, value) in props.iteritems():
                element.set_property(key, value)
        self.add(element)
        return element


class AudioTranscoder(TranscodeBin):
    def __init__(self, d):
        super(AudioTranscoder, self).__init__(d)

        # Create processing elements:
        self._conv = self._make('audioconvert')
        self._rsp = self._make('audioresample', {'quality': 10})
        self._rate = self._make('audiorate')

        # Link elements:
        self._q1.link(self._conv)
        self._conv.link(self._rsp)
        if d.get('caps'):
            # FIXME: There is probably a better way to do this, but the caps API
            # has always been a bit of a mystery to me.  --jderose
            if d['enc'] == 'vorbisenc':
                mime = 'audio/x-raw-float'
            else:
                mime = 'audio/x-raw-int'
            caps = Gst.caps_from_string(
                caps_string(mime, d['caps'])
            )
            self._rsp.link_filtered(self._rate, caps)
        else:
            self._rsp.link(self._rate)
        self._rate.link(self._enc)


class VideoTranscoder(TranscodeBin):
    def __init__(self, d):
        super(VideoTranscoder, self).__init__(d)

        # Create processing elements:
        self._scale = self._make('ffvideoscale', {'method': 10})
        self._q = self._make('queue')

        # Link elements:
        self._q1.link(self._scale)
        if d.get('caps'):
            caps = Gst.caps_from_string(
                caps_string('video/x-raw-yuv', d['caps'])
            )
            self._scale.link_filtered(self._q, caps)
        else:
            self._scale.link(self._q)
        self._q.link(self._enc)


class Transcoder(object):
    def __init__(self, job, fs):
        """
        Initialize.

        :param job: a ``dict`` describing the transcode to perform.
        :param fs: a `FileStore` instance in which to store transcoded file
        """
        if not isinstance(job, dict):
            raise TypeError(
                TYPE_ERROR % ('job', dict, type(job), job)
            )
        if not isinstance(fs, FileStore):
            raise TypeError(
                TYPE_ERROR % ('fs', FileStore, type(fs), fs)
            )
        self.job = job
        self.fs = fs

        src = job['src']
        src_filename = self.fs.path(src['id'], src.get('ext'))
        self.dst_fp = self.fs.allocate_for_write(job.get('ext'))

        self.mainloop = GObject.MainLoop()
        self.pipeline = Gst.Pipeline()

        # Create bus and connect several handlers
        self.bus = self.pipeline.get_bus()
        self.bus.add_signal_watch()
        self.bus.connect('message::eos', self.on_eos)
        self.bus.connect('message::error', self.on_error)

        # Create elements
        self.src = Gst.ElementFactory.make('filesrc', None)
        self.dec = Gst.ElementFactory.make('decodebin2', None)
        self.mux = Gst.ElementFactory.make(job['mux'], None)
        self.sink = Gst.ElementFactory.make('fdsink', None)

        # Set properties
        self.src.set_property('location', src_filename)
        self.sink.set_property('fd', self.dst_fp.fileno())

        # Connect handler for 'new-decoded-pad' signal
        self.dec.connect('new-decoded-pad', self.on_new_decoded_pad)

        # Add elements to pipeline
        for el in (self.src, self.dec, self.mux, self.sink):
            self.pipeline.add(el)

        # Link *some* elements
        # This is completed in self.on_new_decoded_pad()
        self.src.link(self.dec)
        self.mux.link(self.sink)

        self.audio = None
        self.video = None
        self.tup = None

    def run(self):
        self.pipeline.set_state(Gst.State.PLAYING)
        self.mainloop.run()
        return self.tup

    def kill(self):
        self.pipeline.set_state(Gst.State.NULL)
        self.pipeline.get_state()
        self.mainloop.quit()

    def link_pad(self, pad, name, key):
        if key in self.job:
            klass = {'audio': AudioTranscoder, 'video': VideoTranscoder}[key]
            el = klass(self.job[key])
        else:
            el = Gst.ElementFactory.make('fakesink', None)
        self.pipeline.add(el)
        log.info('Linking pad %r with %r', name, el)
        pad.link(el.get_pad('sink'))
        if key in self.job:
            el.link(self.mux)
        el.set_state(Gst.State.PLAYING)
        return el

    def on_new_decoded_pad(self, element, pad, last):
        name = pad.get_caps().to_string()
        log.debug('new decoded pad: %r', name)
        if name.startswith('audio/'):
            assert self.audio is None
            self.audio = self.link_pad(pad, name, 'audio')
        elif name.startswith('video/'):
            assert self.video is None
            self.video = self.link_pad(pad, name, 'video')

    def on_eos(self, bus, msg):
        log.info('eos')
        self.kill()
        self.dst_fp.close()
        fp = open(self.dst_fp.name, 'rb')
        self.tup = self.fs.tmp_hash_move(fp, self.job.get('ext'))

    def on_error(self, bus, msg):
        error = msg.parse_error()[1]
        log.error(error)
        self.kill()
