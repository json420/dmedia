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

NOTE THAT DMEDIA IS NOT YET PRODUCTION READY! THERE WILL STILL BE CHANGES THAT
WILL BREAK YOUR DMEDIA DATABASE AND COULD CAUSE DATA LOSS!
"""

__version__ = '12.07.0'
BUS = 'org.freedesktop.Dmedia'


def configure_logging():
    import sys
    import os
    from os import path
    import logging

    script = path.abspath(sys.argv[0])
    namespace = path.basename(script)
    format = [
        '%(levelname)s',
        '%(processName)s',
        '%(threadName)s',
        '%(message)s',
    ]
    home = path.abspath(os.environ['HOME'])
    if not path.isdir(home):
        raise Exception('$HOME is not a directory: {!r}'.format(home))
    cache = path.join(home, '.cache', 'dmedia')
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
    logging.info('script: %r', script)
    logging.info('dmedia.__file__: %r', __file__)
    logging.info('dmedia.__version__: %r', __version__)


def get_dmedia_dir():
    import os
    from os import path
    home = path.abspath(os.environ['HOME'])
    if not path.isdir(home):
        raise Exception('$HOME is not a directory: {!r}'.format(home))
    share = path.join(home, '.local', 'share', 'dmedia')
    if not path.exists(share):
        os.makedirs(share)

    # FIXME: Migration hack that should do away in 12.02:
    databases = path.join(share, 'databases')
    if not path.exists(databases):
        os.mkdir(databases)
        dc3 = path.join(home, '.local', 'share', 'dc3')
        if path.isdir(dc3):
            import shutil
            for name in os.listdir(dc3):
                if name.startswith('_') or not name.endswith('.couch'):
                    continue
                src = path.join(dc3, name)
                if not path.isfile(src):
                    continue
                dst = path.join(databases, name)
                if not path.exists(dst):
                    shutil.copy2(src, dst)

    return share
