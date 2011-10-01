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

from math import log, floor
from gettext import gettext as _
from gettext import ngettext

from gi.repository import GObject
try:
    from gi.repository.Notify import Notification
except ImportError:
    Notification = None

from dmedia.constants import TYPE_ERROR, CALLABLE_ERROR


UNITS_BASE10 = (
    'bytes',
    'kB',
    'MB',
    'GB',
    'TB',
    'PB',
    # For now, we're capping at 999 PB
    #'EB',
    #'ZB',
    #'YB',
)


def units_base10(size):
    """
    Return *size* bytes to 3 significant digits in SI base-10 units.

    For example:

    >>> units_base10(1000)
    '1 kB'
    >>> units_base10(29481537)
    '29.5 MB'
    >>> units_base10(392012353)
    '392 MB'

    For additional details, see:

        https://wiki.ubuntu.com/UnitsPolicy
    """
    if size is None:
        return None
    if size < 0:
        raise ValueError('size must be greater than zero; got %r' % size)
    if size >= 10 ** 18:
        raise ValueError('size must be smaller than 10**18; got %r' % size)
    if size == 0:
        return '0 bytes'
    if size == 1:
        return '1 byte'
    i = int(floor(log(size, 1000)))
    s = (size / (1000.0 ** i) if i > 0 else size)
    return (
        '%.*g %s' % (3, s, UNITS_BASE10[i])
    )


def import_started(bases):
    """
    Return notification (summary, body) for when card is inserted.

    For example, with a single import:

    >>> import_started(['/media/EOS_DIGITAL'])
    ('Searching for new files...', '/media/EOS_DIGITAL')

    Or with multiple parallel imports in a batch:

    >>> import_started(['/media/EOS_DIGITAL', '/media/OTHER_CARD'])
    ('Searching on 2 cards...', '/media/EOS_DIGITAL\\n/media/OTHER_CARD')

    For details on pro file import UX design, see:

        https://wiki.ubuntu.com/AyatanaDmediaLovefest
    """
    msg = ngettext(
        'Searching for new files...',
        'Searching on %(count)d cards...',
        len(bases)
    )
    summary = msg % dict(count=len(bases))
    body = '\n'.join(bases)
    return (summary, body)


def batch_finished(stats):
    """
    Return notification (summary, body) as per pro file import UX design.

    For example:

    >>> stats = dict(
    ...     imported=9,
    ...     imported_bytes=392012353,
    ...     skipped=1,
    ...     skipped_bytes=29481537,
    ... )
    >>> batch_finished(stats)
    ('Added 9 new files, 392 MB', 'Skipped 1 duplicate, 29.5 MB')

    For details on pro file import UX design, see:

        https://wiki.ubuntu.com/AyatanaDmediaLovefest
    """
    imported = stats.get('imported', 0)
    skipped = stats.get('skipped', 0)
    if imported < 0:
        raise ValueError("stats['imported'] must be >= 0; got %r" % imported)
    if skipped < 0:
        raise ValueError("stats['skipped'] must be >= 0; got %r" % skipped)
    if imported == 0 and skipped == 0:
        return (_('No files found'), None)
    if imported > 0:
        msg = ngettext(
            'Added %(count)d new file, %(size)s',
            'Added %(count)d new files, %(size)s',
            imported
        )
        summary = msg % dict(
            count=imported,
            size=units_base10(stats.get('imported_bytes', 0))
        )
    else:
        summary = _('No new files found')
    if skipped > 0:
        msg = ngettext(
            'Skipped %(count)d duplicate, %(size)s',
            'Skipped %(count)d duplicates, %(size)s',
            skipped
        )
        body = msg % dict(
            count=skipped,
            size=units_base10(stats.get('skipped_bytes', 0))
        )
    else:
        body = None
    return (summary, body)


class NotifyManager(object):
    """
    Helper class to make it easier to update notification when still visible.
    """

    def __init__(self, klass=None):
        self._klass = (Notification if klass is None else klass)
        self._current = None

    def _on_closed(self, notification):
        assert self._current is notification
        self._current = None

    def isvisible(self):
        """
        Return ``True`` in a notification is currently visible.
        """
        return (self._current is not None)

    def notify(self, summary, body=None, icon=None):
        """
        Display a notification with *summary*, *body*, and *icon*.
        """
        assert self._current is None
        self._current = self._klass.new(summary, body, icon)
        self._current.connect('closed', self._on_closed)
        self._current.show()

    def update(self, summary, body=None, icon=None):
        """
        Update current notification to have *summary*, *body*, and *icon*.

        This method will only work if the current notification is still visible.

        To a display new or replace the existing notification regardless whether
        the current notification is visible, use
        `NotifyManager.replace()`.
        """
        assert self._current is not None
        self._current.update(summary, body, icon)
        self._current.show()

    def replace(self, summary, body=None, icon=None):
        """
        Update current notification if visible, otherwise display a new one.
        """
        if self.isvisible():
            self.update(summary, body, icon)
        else:
            self.notify(summary, body, icon)


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