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
