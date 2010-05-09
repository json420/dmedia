# Authors:
#   Jason Gerard DeRose <jderose@jasonderose.org>
#
# media: distributed media library
# Copyright (C) 2010 Jason Gerard DeRose <jderose@jasonderose.org>
#
# This file is part of `media`.
#
# `media` is free software: you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# `media` is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License along
# with `media`.  If not, see <http://www.gnu.org/licenses/>.

"""
Some test helpers.
"""

import os
from os import path
from subprocess import check_call
import tempfile
import shutil


class ExceptionNotRaised(StandardError):
    """
    Raised when an expected exception is not raised.
    """

    def __init__(self, expected):
        self.expected = expected
        StandardError.__init__(self, 'expected %s' % expected.__name__)


def raises(exception, callback, *args, **kw):
    """
    Test that ``exception`` is raised when ``callback`` is called.
    """
    raised = False
    try:
        callback(*args, **kw)
    except exception, e:
        raised = True
    if not raised:
        raise ExceptionNotRaised(exception)
    return e


class TempDir(object):
    def __init__(self, prefix='unit-tests.'):
        self.__prefix = prefix
        self.__path = tempfile.mkdtemp(prefix=self.__prefix)
        assert self.path == self.__path

    def __iter__(self):
        for name in sorted(os.listdir(self.path)):
            yield name

    def __get_path(self):
        assert path.abspath(self.__path) == self.__path
        assert self.__path.startswith(path.join('/tmp', self.__prefix))
        assert path.isdir(self.__path) and not path.islink(self.__path)
        return self.__path
    path = property(__get_path)

    def rmtree(self):
        if self.__path is not None:
            check_call(['chmod', '-R', '+w', self.path])
            shutil.rmtree(self.path)
            self.__path = None

    def makedirs(self, *parts):
        d = self.join(*parts)
        if not path.exists(d):
            os.makedirs(d)
        assert path.isdir(d) and not path.islink(d)
        return d

    def touch(self, *parts):
        d = self.makedirs(*parts[:-1])
        f = path.join(d, parts[-1])
        assert not path.exists(f)
        open(f, 'w').close()
        assert path.isfile(f) and not path.islink(f)
        return f

    def write(self, content, *parts):
        d = self.makedirs(*parts[:-1])
        f = path.join(d, parts[-1])
        assert not path.exists(f)
        open(f, 'w').write(content)
        assert path.isfile(f) and not path.islink(f)
        return f

    def join(self, *parts):
        return path.join(self.path, *parts)

    def __del__(self):
        self.rmtree()


class TempHome(TempDir):
    def __init__(self):
        super(TempHome, self).__init__()
        self.orig = os.environ['HOME']
        os.environ['HOME'] = self.path

    def __del__(self):
        os.environ['HOME'] = self.orig
        super(TempHome, self).__del__()
