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
Small abstraction to hide desktopcouch vs system-wide CouchDB details.

dmedia is obviously very focused on desktop use, for which desktopcouch is a
godsend.  However, dmedia also needs to run on headless, minimal servers.  We
want to abstract the difference to a single place in the code so the rest of the
code can just work without concern for whether we're connecting to a CouchDB
instance launched by desktopcouch, or a system-wide CouchDB started with an
init script.

This would be a great feature to upstream into desktopcouch, but for now we can
give it a good trial by fire in dmedia.

For more details, see:

    https://bugs.launchpad.net/dmedia/+bug/722035
"""

import json
from subprocess import check_output
import logging

from microfiber import Database  # Hooray!


log = logging.getLogger()


def get_db(env):
    """
    Return the CouchDB database specified by *env*.

    The database name is determined by ``env['dbname']``.

    Returns a ``microfiber.Database`` instance.
    """
    dbname = env['dbname']
    log.info('CouchDB database is %r', dbname)
    return Database(dbname, env['url'], env.get('oauth'), env.get('basic'))


def get_env(dbname='dmedia'):
    env_s = check_output(['/usr/bin/dc3-control', 'GetEnv'])
    env = json.loads(env_s)
    # FIXME: oauth is broken in CouchDB on Oneiric
    env['oauth'] = None
    env['dbname'] = dbname
    return env
