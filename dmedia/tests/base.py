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
Usefull TestCase subclasses.
"""

from unittest import TestCase
from os import path
from base64 import b64decode
import os
from os import path
import tempfile
import shutil

from filestore import ContentHash


datadir = path.join(path.dirname(path.abspath(__file__)), 'data')


class SampleFilesTestCase(TestCase):
    """
    Base clase for tests that use the files in dmedia/tests/data.

    If the MVI_5751.MOV or MVI_5751.THM file isn't present, self.skipTest() is
    called.  This will allow us to stop shipping the 20MB video file in the
    dmedia release tarballs.
    """

    mov = path.join(datadir, 'MVI_5751.MOV')
    thm = path.join(datadir, 'MVI_5751.THM')
    mov_ch = ContentHash(
        'YGDV257NS4727MLMM52YPRFME7YWIUEFDZC6XMRKMBMDQ2DV',
        20202333,
        b64decode(b''.join([
            b'yo0WOxW2f7lieV7zIuAhZBBX7dNhAISx1cKC4Izc',
            b'/IFZSXhWaIUtYrLwxb/WpXE/m0anfUmtIujXdTM2',
            b'/J1zUUpuAQCIQF92Q2WM5iblkiM4wKEEhnq3CJsO'
        ]))
    )
    thm_ch = ContentHash(
        'TZE6TOCGTZSNXFANERWF2VH2GMV6REUSBKPHOLJCVHDS6UF6',
        27328,
        b64decode(b'Hme1V45dR/uBMSKsa9GZuHkwYfwzBPELDNma35VN'),   
    )

    def setUp(self):
        for filename in (self.mov, self.thm):
            if not path.isfile(filename):
                self.skipTest('Missing file {!r}'.format(filename))


class TempDir(object):
    def __init__(self):
        self.dir = tempfile.mkdtemp(prefix='unittest.')

    def __del__(self):
        self.rmtree()

    def rmtree(self):
        if self.dir is not None:
            shutil.rmtree(self.dir)
            self.dir = None

    def join(self, *parts):
        return path.join(self.dir, *parts)

    def makedirs(self, *parts):
        d = self.join(*parts)
        if not path.exists(d):
            os.makedirs(d)
        assert path.isdir(d), d
        return d

    def touch(self, *parts):
        self.makedirs(*parts[:-1])
        f = self.join(*parts)
        open(f, 'wb').close()
        return f

    def write(self, data, *parts):
        self.makedirs(*parts[:-1])
        f = self.join(*parts)
        open(f, 'wb').write(data)
        return f

    def copy(self, src, *parts):
        self.makedirs(*parts[:-1])
        dst = self.join(*parts)
        shutil.copy(src, dst)
        return dst

