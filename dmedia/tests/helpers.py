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
Some test helpers.
"""

from unittest import TestCase
import os
from os import path
from subprocess import check_call
import tempfile
import shutil
from base64 import b32encode, b32decode, b64encode

from . import sample_mov, sample_thm

mov_hash = 'TGX33XXWU3EVHEEY5J7NBOJGKBFXLEBK'
mov_leaves = [
    b32decode('IXJTSUCYYFECGSG6JIB2R77CAJVJK4W3'),
    b32decode('MA3IAHUOKXR4TRG7CWAPOO7U4WCV5WJ4'),
    b32decode('FHF7KDMAGNYOVNYSYT6ZYWQLUOCTUADI'),
]
mov_att = {
    'data': b64encode(''.join(mov_leaves)),
    'content_type': 'application/octet-stream',
}

mov_qid = 'GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN'

thm_hash = 'GKZMOPVZILR43MZCXLVYP7T62XGBT7BQ'
thm_leaves = [b32decode('F6ATTKI6YVWVRBQQESAZ4DSUXQ4G457A')]
thm_qid = 'EYCDXXCNDB6OIIX5DN74J7KEXLNCQD5M'


def random_bus():
    random = 'test' + b32encode(os.urandom(10))  # 80-bits of entropy
    return '.'.join(['org', random, 'DMedia'])


def prep_import_source(tmp):
    src1 = tmp.copy(sample_mov, 'DCIM', '100EOS5D2', 'MVI_5751.MOV')
    src2 = tmp.copy(sample_thm, 'DCIM', '100EOS5D2', 'MVI_5751.THM')
    dup1 = tmp.copy(sample_mov, 'DCIM', '100EOS5D2', 'MVI_5752.MOV')
    return (src1, src2, dup1)


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
    def __init__(self, prefix='unit-tests.', dir=None):
        self.__prefix = prefix
        self.__path = tempfile.mkdtemp(prefix=self.__prefix, dir=dir)
        assert self.path == self.__path

    def __iter__(self):
        for name in sorted(os.listdir(self.path)):
            yield name

    def __get_path(self):
        assert path.abspath(self.__path) == self.__path
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

    def copy(self, src, *parts):
        self.makedirs(*parts[:-1])
        dst = self.join(*parts)
        assert not path.exists(dst)
        shutil.copy2(src, dst)
        assert path.isfile(dst) and not path.islink(dst)
        return dst

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


class DummyQueue(object):
    def __init__(self):
        self.messages = []

    def put(self, msg):
        self.messages.append(msg)


class DummyCallback(object):
    def __init__(self):
        self.messages = []

    def __call__(self, signal, args):
        self.messages.append((signal, args))
