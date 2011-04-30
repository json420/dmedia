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
`dmedia` - distributed media library

WARNING: the dmedia content-hash and schema are *not* yet stable, may change
wildly and without warning!

The `dmedia` API will go through significant changes in the next few months,
so keep your hardhats on!  A good place to start is the `FileStore` class in the
`filestore` module, which also probably has the most stable API of any of the
current code.
"""

__version__ = '0.7.0'


def configure_logging(namespace):
    import os
    from os import path
    import logging

    import xdg.BaseDirectory

    format = [
        '%(levelname)s',
        '%(process)d',
        '%(message)s',
    ]
    cache = path.join(xdg.BaseDirectory.xdg_cache_home, 'dmedia')
    if not path.exists(cache):
        os.makedirs(cache)
    filename = path.join(cache, namespace + '.log')
    if path.exists(filename):
        os.rename(filename, filename + '.previous')
    logging.basicConfig(
        filename=filename,
        filemode='w',
        level=logging.DEBUG,
        format='\t'.join(format),
    )
    logging.info('dmedia.__version__: %r', __version__)
    logging.info('dmedia.__file__: %r', __file__)
    logging.info('Logging namespace %r to %r', namespace, filename)
