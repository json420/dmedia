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
Custom exceptions
"""

class DmediaError(StandardError):
    """
    Base class for all custom dmedia exceptions.
    """

    _format = ''

    def __init__(self, **kw):
        self._kw = kw
        for (key, value) in kw.iteritems():
            assert not hasattr(self, key), 'conflicting kwarg %s.%s = %r' % (
                self.__class__.__name__, key, value,
            )
            setattr(self, key, value)
        super(DmediaError, self).__init__(self._format % kw)


class AmbiguousPath(DmediaError):
    _format = '%(pathname)r resolves to %(abspath)r'


class FileStoreTraversal(DmediaError):
    """
    Raised when what should be internal path traverses out of FileStore base.

    For example:

    >>> raise FileStoreTraversal(pathname='/foo/barNone/baz', base='/foo/bar')
    Traceback (most recent call last):
      ...
    FileStoreTraversal: '/foo/barNone/baz' outside base '/foo/bar'
    """
    _format = '%(pathname)r outside base %(base)r'


class DuplicateFile(DmediaError):
    _format = 'chash=%(chash)r, src=%(src)r, dst=%(dst)r'


class DownloadFailure(DmediaError):
    _format = 'leaf %(leaf)d expected %(expected)r; got %(got)r'
