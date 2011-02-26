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

import gobject
import gst


log = logging.getLogger()


class TranscodeBin(gst.Bin):
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
            gst.GhostPad('sink', self._q1.get_pad('sink'))
        )
        self.add_pad(
            gst.GhostPad('src', self._q2.get_pad('src'))
        )

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self._d)

    def _make(self, name, props=None):
        """
        Create gst element, set properties, and add to this bin.
        """
        element = gst.element_factory_make(name)
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
            caps = gst.caps_from_string(d['caps'])
            self._rsp.link(self._rate, caps)
        else:
            self._rsp.link(self._rate)
        self._rate.link(self._enc)


class VideoTranscoder(TranscodeBin):
    def __init__(self, d):
        super(VideoTranscoder, self).__init__(d)

        # Create processing elements:
        self._scale = self._make('videoscale', {'method': 2})
        self._q = self._make('queue')

        # Link elements:
        self._q1.link(self._scale)
        if d.get('caps'):
            caps = gst.caps_from_string(d['caps'])
            self._scale.link(self._q, caps)
        else:
            self._scale.link(self._q)
        self._q.link(self._enc)


class Transcoder(object):
    def __init__(self, src, dst, d):
        self.d = d
        self.mainloop = gobject.MainLoop()
        self.pipeline = gst.Pipeline()

        # Create bus and connect several handlers
        self.bus = self.pipeline.get_bus()
        self.bus.add_signal_watch()
        self.bus.connect('message::eos', self.on_eos)
        self.bus.connect('message::error', self.on_error)

        # Create elements
        self.src = gst.element_factory_make('filesrc')
        self.dec = gst.element_factory_make('decodebin2')
        self.mux = gst.element_factory_make(d['mux'])
        self.sink = gst.element_factory_make('filesink')

        # Set properties
        self.src.set_property('location', src)
        self.sink.set_property('location', dst)

        # Connect handler for 'new-decoded-pad' signal
        self.dec.connect('new-decoded-pad', self.on_new_decoded_pad)

        # Add elements to pipeline
        self.pipeline.add(self.src, self.dec, self.mux, self.sink)

        # Link *some* elements
        # This is completed in self.on_new_decoded_pad()
        self.src.link(self.dec)
        self.mux.link(self.sink)

        self.audio = None
        self.video = None

    def run(self):
        self.pipeline.set_state(gst.STATE_PLAYING)
        self.mainloop.run()

    def kill(self):
        self.pipeline.set_state(gst.STATE_NULL)
        self.pipeline.get_state()
        self.mainloop.quit()

    def link_pad(self, pad, name, key):
        if key in self.d:
            klass = {'audio': AudioTranscoder, 'video': VideoTranscoder}[key]
            el = klass(self.d[key])
        else:
            el = gst.element_factory_make('fakesink')
        self.pipeline.add(el)
        log.info('Linking pad %r with %r', name, el)
        pad.link(el.get_pad('sink'))
        if key in self.d:
            el.link(self.mux)
        el.set_state(gst.STATE_PLAYING)
        return el

    def on_new_decoded_pad(self, element, pad, last):
        name = pad.get_caps()[0].get_name()
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

    def on_error(self, bus, msg):
        error = msg.parse_error()[1]
        log.error(error)
        self.kill()
