# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#
# dmedia: distributed media library
# Copyright (C) 2010 Jason Gerard DeRose <jderose@novacut.com>
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
Misc. utility functions and classes.
"""

from gi.repository import GObject

from dmedia.constants import TYPE_ERROR, CALLABLE_ERROR


class Timer(object):
    def __init__(self, seconds, callback):
        if not isinstance(seconds, (float, int)):
            raise TypeError(
                TYPE_ERROR % ('seconds', (float, int), type(seconds), seconds)
            )
        if seconds <= 0:
            raise ValueError(
                'seconds: must be > 0; got %r' % seconds
            )
        if not callable(callback):
            raise TypeError(
                CALLABLE_ERROR % ('callback', type(callback), callback)
            )
        self.seconds = seconds
        self.callback = callback
        self.__timeout_id = None

    def __on_timeout(self):
        self.callback()
        return True  # Repeat timeout call

    def start(self):
        if self.__timeout_id is not None:
            return False
        self.__timeout_id = GObject.timeout_add(
            int(self.seconds * 1000),
            self.__on_timeout
        )
        return True

    def stop(self):
        if self.__timeout_id is None:
            return False
        GObject.source_remove(self.__timeout_id)
        self.__timeout_id = None
        return True

    def restart(self):
        if self.stop():
            return self.start()
        return False
