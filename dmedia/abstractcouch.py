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

import logging

from couchdb import Server, ResourceNotFound
try:
    from desktopcouch.records.http import OAuthSession
except ImportError:
    OAuthSession = None
if OAuthSession is not None:
    from desktopcouch.application.platform import find_port
    from desktopcouch.application.local_files import get_oauth_tokens


log = logging.getLogger()


def get_couchdb_server(env):
    """
    Return `couchdb.Server` for desktopcouch or system-wide CouchDB.

    The *env* argument is a ``dict`` instance containing information about how
    to connect to CouchDB.  It must always contain a ``"url"`` key, which is the
    URL as passed to ``couchdb.Server()``.

    If *env* contains an ``"oauth"`` key, a
    ``desktopcouch.records.http.OAuthSession`` will be created and passed to
    ``couchdb.Server()``.

    For example, to connect to the system-wide CouchDB, pass an *env* like this:

    >>> env = {'url': 'http://localhost:5984/'}

    Or to connect to a per-user desktopcouch CouchDB, pass an *env* like this:

    >>> env = {
    ...     'url': 'http://localhost:51074/',
    ...     'oauth': {
    ...         'consumer_secret': 'no',
    ...         'token': 'way',
    ...         'consumer_key': 'too',
    ...         'token_secret': 'secret'
    ...     }
    ... }

    When using desktopcouch, you can build an *env* with the correct port and
    oauth credentials like this:

    >>> from desktopcouch.application.platform import find_port
    >>> from desktopcouch.application.local_files import get_oauth_tokens
    >>> env = {
    ...     'url': 'http://localhost:%d/' % find_port(),
    ...     'oauth': get_oauth_tokens(),
    ... }

    Note the reason *env* is a ``dict`` is so it's easily extensible and can
    carry other useful information, for example the dmedia database name so that
    an alternate name can be provided for, say,  unit tests (like
    ``"dmedia_test"``).

    The goal is to have all the needed information is one easily serialized
    piece of data (important for testing across multiple processes).
    """
    url = env.get('url', 'http://localhost:5984/')
    log.info('CouchDB server is %r', url)
    if env.get('oauth') is None:
        session = None
    else:
        if OAuthSession is None:
            raise ValueError(
                "provided env['oauth'] but OAuthSession not available: %r" %
                    (env,)
            )
        log.info('Using desktopcouch `OAuthSession`')
        session = OAuthSession(credentials=env['oauth'])
    return Server(url, session=session)


def get_dmedia_db(env, server=None):
    """
    Return the dmedia database specified by *env*.

    The database name is determined by ``env['dbname']``.  If the ``"dbname"``
    key is missing or is ``None``, the default database name ``"dmedia"`` is
    used.

    If the database does not exist, it will be created.

    If *server* is ``None``, on is created with *env* by calling
    `get_couchdb_server()`.

    Returns a ``couchdb.Database`` instance.
    """
    if server is None:
        server = get_couchdb_server(env)
    dbname = env.get('dbname')
    if dbname is None:
        dbname = 'dmedia'
    log.info('CouchDB database is %r', dbname)
    try:
        return server[dbname]
    except ResourceNotFound:
        return server.create(dbname)


def get_env(dbname=None):
    """
    Return default *env*.

    This will return an appropriate *env* based on whether desktopcouch is
    available.  Not a perfect solution, but works for now.
    """
    if OAuthSession is None:
        return {'dbname': dbname}
    port = find_port()
    return {
        'port': port,
        'url': 'http://localhost:%d/' % port,
        'oauth': get_oauth_tokens(),
        'dbname': dbname,
    }
