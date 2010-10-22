# Authors:
#   Jason Gerard DeRose <jderose@jasonderose.org>
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
Unit tests for `dmedialib.ui` module.
"""

from os import path
from genshi.template import MarkupTemplate
from dmedialib import ui, datadir


def test_datafile():
    f = ui.datafile
    assert f('foo.xml') == path.join(datadir, 'foo.xml')


def test_load_datafile():
    f = ui.load_datafile
    mootools = path.join(datadir, 'mootools.js')
    assert f('mootools.js') == open(mootools, 'r').read()


def test_load_template():
    f = ui.load_template
    xml = path.join(datadir, 'browser.xml')
    t = f('browser.xml')
    assert isinstance(t, MarkupTemplate)
    assert t.filename == xml


def test_render_template():
    f = ui.render_template
    t = ui.load_template('browser.xml')
    s = f(t)
    assert isinstance(s, str)
    assert s.startswith('<!DOCTYPE html PUBLIC')
