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
Low level utility functions for working with templates and data files.
"""

from os import path
import json

from genshi.template import MarkupTemplate


DATADIR = path.join(path.dirname(path.abspath(__file__)), 'data')
assert path.isdir(DATADIR)


def render_var(name, obj, indent=None):
    """
    Render *obj* to JavaScript variable *name*.

    For example:

    >>> render_var('dmedia', dict(hello='world', foo='bar'))
    'var dmedia = {"foo": "bar", "hello": "world"};'

    Also works for object attribute assignment:

    >>> render_var('dmedia.data', dict(hello='world', foo='bar'))
    'dmedia.data = {"foo": "bar", "hello": "world"};'

    """
    if not obj:
        return None
    t = ('{} = {};' if '.' in name else 'var {} = {};')
    return t.format(name, json.dumps(obj, sort_keys=True, indent=indent))


def datafile(name, parent=None):
    """
    Return full path of data file *name* in directory *parent*.

    If *parent* is ommited or None, the default dmedia.webui package data
    directory is used.
    """
    parent = (DATADIR if parent is None else parent)
    return path.join(parent, name)


def load_data(filename):
    """
    Load the contents of the file at *filename*.
    """
    return open(filename, 'rb').read()


def load_template(filename):
    """
    Load a Genshi XML template from file at *filename*.
    """
    data = open(filename, 'rb').read()
    return MarkupTemplate(data, filepath=filename)
