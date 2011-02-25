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


import gst


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


class AudioTranscoder(gst.Bin):
    def __init__(self, d):
        gst.Bin.__init__(self)

        # Create elements
        self._q1 = gst.element_factory_make('queue')
        self._conv = gst.element_factory_make('audioconvert')
        self._rate = gst.element_factory_make('audiorate')
        self._enc = make_encoder(d)
        self._q2 = gst.element_factory_make('queue')
        elements = (self._q1, self._conv, self._rate, self._enc, self._q2)

        # Add to bin and link:
        self.add(*elements)
        if d.get('caps'):
            gst.element_link_many(self._q1, self._conv, self._rate)
            caps = gst.caps_from_string(d['caps'])
            self._rate.link(self._enc, caps)
            self._enc.link(self._q2)
        else:
            gst.element_link_many(*elements)

        # Add ghostpads
        self.add_pad(gst.GhostPad('sink', self._q1.get_pad('sink')))
        self.add_pad(gst.GhostPad('src', self._q2.get_pad('src')))


class VideoTranscoder(gst.Bin):
    def __init__(self, d):
        gst.Bin.__init__(self)

        # Create elements
        self._q1 = gst.element_factory_make('queue')
        self._scale = gst.element_factory_make('videoscale')
        self._scale.set_property('method', 2)
        self._enc = make_encoder(d)
        self._q2 = gst.element_factory_make('queue')
        elements = (self._q1, self._scale, self._enc, self._q2)

        # Add to bin and link:
        self.add(*elements)
        if d.get('size'):
            self._q1.link(self._scale)
            caps = gst.caps_from_string(
                'video/x-raw-yuv, width=%(width)d, height=%(height)d' % d['size']
            )
            self._scale.link(self._enc, caps)
            self._enc.link(self._q2)
        else:
            gst.element_link_many(*elements)

        # Add ghostpads
        self.add_pad(gst.GhostPad('sink', self._q1.get_pad('sink')))
        self.add_pad(gst.GhostPad('src', self._q2.get_pad('src')))
