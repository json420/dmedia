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

import os
from os import path
from subprocess import check_call
import tempfile
import shutil


datadir = path.join(path.dirname(path.abspath(__file__)), 'data')

sample_mov = path.join(datadir, 'MVI_5751.MOV')
sample_mov_hash = 'OMLUWEIPEUNRGYMKAEHG3AEZPVZ5TUQE'
sample_mov_qid = 'GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN'

sample_thm = path.join(datadir, 'MVI_5751.THM')
sample_thm_hash = 'F6ATTKI6YVWVRBQQESAZ4DSUXQ4G457A'
sample_thm_qid = 'EYCDXXCNDB6OIIX5DN74J7KEXLNCQD5M'

assert path.isdir(datadir)
assert path.isfile(sample_mov)
assert path.isfile(sample_thm)


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



DVALUE = """assert_deepequal: expected != got.
  %s
  expected:
%r
  got:
%r
  path = %r"""

DTYPE = """assert_deepequal: type(expected) is not type(got).
  %s
  type(expected) = %r
  type(got) = %r
  expected = %r
  got = %r
  path = %r"""

DLEN = """assert_deepequal: list length mismatch.
  %s
  len(expected) = %r
  len(got) = %r
  expected = %r
  got = %r
  path = %r"""

DKEYS = """assert_deepequal: dict keys mismatch.
  %s
  missing keys = %r
  extra keys = %r
  expected =%r
  got = %r
  path = %r"""



def assert_deepequal(expected, got, doc='', stack=tuple()):
    """
    Recursively check for type and equality.

    If the tests fails, it will raise an ``AssertionError`` with detailed
    information, including the path to the offending value.  For example:

    >>> expected = [u'Hello', dict(world=u'how are you?')]
    >>> got = [u'Hello', dict(world='how are you?')]
    >>> expected == got
    True
    >>> assert_deepequal(expected, got, doc='Testing my nested data')
    Traceback (most recent call last):
      ...
    AssertionError: assert_deepequal: type(expected) is not type(got).
      Testing my nested data
      type(expected) = <type 'unicode'>
      type(got) = <type 'str'>
      expected = u'how are you?'
      got = 'how are you?'
      path = (1, 'world')
    """
    if type(expected) is not type(got):
        raise AssertionError(
            DTYPE % (doc, type(expected), type(got), expected, got, stack)
        )
    if isinstance(expected, (list, tuple)):
        if len(expected) != len(got):
            raise AssertionError(
                DLEN % (doc, len(expected), len(got), expected, got, stack)
            )
        for (i, e_sub) in enumerate(expected):
            g_sub = got[i]
            assert_deepequal(e_sub, g_sub, doc, stack + (i,))
    elif isinstance(expected, dict):
        missing = set(expected).difference(got)
        extra = set(got).difference(expected)
        if missing or extra:
            raise AssertionError(DKEYS % (
                    doc, sorted(missing), sorted(extra), expected, got, stack
                )
            )
        for key in sorted(expected):
            e_sub = expected[key]
            g_sub = got[key]
            assert_deepequal(e_sub, g_sub, doc, stack + (key,))
    elif expected != got:
        raise AssertionError(
            DVALUE % (doc, expected, got, stack)
        )
