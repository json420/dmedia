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

from os import path

__version__ = '13.01.0'
BUS = 'org.freedesktop.Dmedia'
TREE = path.dirname(path.dirname(path.abspath(__file__)))


def configure_logging():
    import sys
    import os
    from os import path
    import logging
    import platform

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
    log = logging.getLogger()
    log.info('======== Dmedia Process Start ========')
    log.info('script: %r', script)
    log.info('__file__: %r', __file__)
    log.info('__version__: %r', __version__)
    log.info('Python: %s, %s, %s',
        platform.python_version(), platform.machine(), platform.system()
    )
    log.info('======================================')
    return log


def get_dmedia_dir():
    import os
    from os import path
    home = path.abspath(os.environ['HOME'])
    if not path.isdir(home):
        raise Exception('$HOME is not a directory: {!r}'.format(home))
    share = path.join(home, '.local', 'share', 'dmedia')
    if not path.exists(share):
        os.makedirs(share)
    return share
