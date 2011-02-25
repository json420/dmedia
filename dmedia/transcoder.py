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


def make_encoder(d):
    """
    Create encoder element from serialized description *d*.

    For example:

    >>> d = {
    ...     'enc': 'theoraenc',
    ...     'props': {
    ...         'quality': 52,
    ...         'keyframe-force': 32,
    ...     },
    ... }
    ...
    >>> enc = make_encoder(d)
    >>> enc.get_factory().get_name()
    'theoraenc'
    >>> enc.get_property('quality')
    52

    :param d: a ``dict`` describing the GStreamer element
    """
    encoder = gst.element_factory_make(d['enc'])
    for (key, value) in d['props'].iteritems():
        encoder.set_property(key, value)
    return encoder


class TranscodeBin(gst.Bin):
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
        element = gst.element_factory_make(name)
        self.add(element)
        if props:
            for (key, value) in props.iteritems():
                element.set_property(key, value)
        return element


class AudioTranscoder(TranscodeBin):
    def __init__(self, d):
        super(AudioTranscoder, self).__init__(d)

        # Create processing elements:
        self._conv = self._make('audioconvert')
        self._rate = self._make('audiorate')

        # Link elements:
        if d.get('caps'):
            gst.element_link_many(self._q1, self._conv, self._rate)
            caps = gst.caps_from_string(d['caps'])
            self._rate.link(self._enc, caps)
        else:
            gst.element_link_many(self._q1, self._conv, self._rate, self._enc)


class VideoTranscoder(TranscodeBin):
    def __init__(self, d):
        super(VideoTranscoder, self).__init__(d)

        # Create processing elements:
        self._scale = self._make('videoscale')
        self._scale.set_property('method', 2)

        # Link elements:
        if d.get('size'):
            self._q1.link(self._scale)
            caps = gst.caps_from_string(
                'video/x-raw-yuv, width=%(width)d, height=%(height)d' % d['size']
            )
            self._scale.link(self._enc, caps)
        else:
            gst.element_link_many(self._q1, self._scale, self._enc)


class Transcoder(object):
    def __init__(self, src, dst, d):
        self.src = src
        self.dst = dst
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

    def on_new_decoded_pad(self, element, pad, last):
        name = pad.get_caps()[0].get_name()
        log.debug('new decoded pad: %r', name)
        if name.startswith('audio/'):
            assert self.audio is None
            if 'audio' in self.d:
                self.audio = AudioTranscoder(self.d['audio'])
            else:
                self.audio = gst.element_factory_make('fakesink')
            self.pipeline.add(self.audio)
            log.info('Linking pad %r with %r', name, self.audio)
            pad.link(self.audio.get_pad('sink'))
            if 'audio' in self.d:
                self.audio.link(self.mux)
            self.audio.set_state(gst.STATE_PLAYING)
        elif name.startswith('video/'):
            assert self.video is None
            if 'video' in self.d:
                self.video = VideoTranscoder(self.d['video'])
            else:
                self.video = gst.element_factory_make('fakesink')
            self.pipeline.add(self.video)
            log.info('Linking pad %r with %r', name, self.video)
            pad.link(self.video.get_pad('sink'))
            if 'video' in self.d:
                self.video.link(self.mux)
            self.video.set_state(gst.STATE_PLAYING)

    def on_eos(self, bus, msg):
        log.info('eos')
        self.kill()

    def on_error(self, bus, msg):
        error = msg.parse_error()[1]
        log.error(error)
        self.kill()
