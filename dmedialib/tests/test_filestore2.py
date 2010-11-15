# Authors:
#   Jason Gerard DeRose <jderose@jasonderose.org>
#   Akshat Jain <ssj6akshat1234@gmail.com>
#
# dmedia: distributed media library
# Copyright (C) 2010 Jason Gerard DeRose <jderose@jasonderose.org>
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# `dmedia` is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License along
# with `dmedia`.  If not, see <http://www.gnu.org/licenses/>.


"""
Unit tests for `dmedialib.filestore` module.
"""

import os
from os import path
import hashlib
from unittest import TestCase
from .helpers import TempDir, TempHome, raises, sample_mov, sample_thm
from dmedialib import filestore2



class test_functions(TestCase):

    def test_hash_file(self):
        f = filestore2.hash_file
        self.assertEqual(f(sample_mov), 'OMLUWEIPEUNRGYMKAEHG3AEZPVZ5TUQE')
        self.assertEqual(f(sample_thm), 'F6ATTKI6YVWVRBQQESAZ4DSUXQ4G457A')

    def test_hash_and_copy(self):
        f = filestore2.hash_and_copy
        hash_file = filestore2.hash_file
        tmp = TempDir()
        dst1 = tmp.join('sample.mov')
        dst2 = tmp.join('sample.thm')
        self.assertEqual(f(sample_mov, dst1), 'OMLUWEIPEUNRGYMKAEHG3AEZPVZ5TUQE')
        self.assertEqual(hash_file(dst1), 'OMLUWEIPEUNRGYMKAEHG3AEZPVZ5TUQE')
        self.assertEqual(f(sample_thm, dst2), 'F6ATTKI6YVWVRBQQESAZ4DSUXQ4G457A')
        self.assertEqual(hash_file(dst2), 'F6ATTKI6YVWVRBQQESAZ4DSUXQ4G457A')

    def test_quick_id(self):
        f = filestore2.quick_id
        self.assertEqual(f(sample_mov), 'GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN')
        self.assertEqual(f(sample_thm), 'EYCDXXCNDB6OIIX5DN74J7KEXLNCQD5M')
