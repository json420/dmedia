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
Base class for CouchDB tests.
"""

from unittest import TestCase
import tempfile
import os
from os import path
from subprocess import Popen
import time
import socket
from hashlib import sha1
from base64 import b32encode
import shutil
from copy import deepcopy

import microfiber

from dmedia.schema import random_id

from .helpers import TempHome


SOCKET_OPTIONS = '[{recbuf, 262144}, {sndbuf, 262144}, {nodelay, true}]'

SESSION_INI = """
[couch_httpd_auth]
require_valid_user = true

[httpd]
bind_address = 127.0.0.1
port = {port}
socket_options = {socket_options}

[couchdb]
view_index_dir = {share}
database_dir = {share}

[log]
file = {logfile}
level = notice

[admins]
{username} = {hashed}

[oauth_token_users]
{token} = {username}

[oauth_token_secrets]
{token} = {token_secret}

[oauth_consumer_secrets]
{consumer_key} = {consumer_secret}

[httpd_global_handlers]
_stats =

[daemons]
stats_collector =
stats_aggregator =

[stats]
rate =
samples =
"""


def random_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    return (s, port)


def random_key():
    return b32encode(os.urandom(10))


def random_oauth():
    return dict(
        (k, random_key())
        for k in ('consumer_key', 'consumer_secret', 'token', 'token_secret')
    )


def random_basic():
    return dict(
        (k, random_key())
        for k in ('username', 'password')
    )


def random_env(port, oauth=True):
    env = {
        'port': port,
        'url': 'http://localhost:{}/'.format(port),
        'basic': random_basic(),
    }
    if oauth:
        env['oauth'] = random_oauth()
    return env


def random_salt():
    return os.urandom(16).encode('hex')


def couch_hashed(password, salt):
    hexdigest = sha1(password + salt).hexdigest()
    return '-hashed-{},{}'.format(hexdigest, salt)


def get_cmd(ini):
    return [
        '/usr/bin/couchdb',
        '-n',  # reset configuration file chain (including system default)
        '-a', '/etc/couchdb/default.ini',
        '-a', ini,
    ]


class TempDir(object):
    def __init__(self):
        self.tmp = tempfile.mkdtemp(prefix='tmpcouch.')
        self.config = self.mkdir('config')
        self.share = self.mkdir('share')
        self.cache = self.mkdir('cache')

    def __del__(self):
        self.rmtree()

    def rmtree(self):
        if self.tmp is None:
            return
        shutil.rmtree(self.tmp)
        self.tmp = None

    def mkdir(self, name):
        d = path.join(self.tmp, name)
        os.mkdir(d)
        return d


class TempCouch(object):
    def __init__(self):
        self.tmp = TempDir()
        self.ini = path.join(self.tmp.config, 'session.ini')
        self.couchdb = None
        self.__bootstraped = False

    def __del__(self):
        self.kill()

    def _build_kw(self, env):
        kw = {
            'share': self.tmp.share,
            'logfile': path.join(self.tmp.cache, 'couchdb.log'),
            'socket_options': SOCKET_OPTIONS,
            'port': env['port'],
            'username': env['basic']['username'],
            'hashed': couch_hashed(env['basic']['password'], random_salt()),
        }
        if env.get('oauth'):
            kw.update(env['oauth'])
        return kw

    def _write_config(self, env):
        kw = self._build_kw(env)
        open(self.ini, 'w').write(SESSION_INI.format(**kw))

    def bootstrap(self, oauth=True):
        assert not self.__bootstraped
        self.__bootstraped = True
        (sock, port) = random_port()
        env = random_env(port, oauth)
        self.server = microfiber.Server(env)
        self._write_config(env)
        sock.close()
        self.start()
        return deepcopy(env)

    def start(self):
        if self.couchdb is not None:
            return False
        self.couchdb = Popen(get_cmd(self.ini))
        time.sleep(0.2)
        t = 0.2
        for i in range(10):
            if self.isalive():
                return True
            time.sleep(t)
            t += 0.1
        raise Exception('could not start CouchDB')

    def isalive(self):
        try:
            self.server.get()
            return True
        except socket.error:
            return False

    def kill(self):
        if self.couchdb is None:
            return False
        self.couchdb.terminate()
        self.couchdb.wait()
        self.couchdb = None
        return True


class CouchCase(TestCase):
    """
    Base class for tests that talk to CouchDB.

    So that a user's production data doesn't get hosed, a CouchDB instance is
    is created for each test case, using temporary files, and destroyed at the
    end of each test case.
    """

    def setUp(self):
        self.tmpcouch = TempCouch()
        self.env = self.tmpcouch.bootstrap()
        self.dbname = 'test_dmedia'
        # All the tests expect the database to exist right now:
        db = microfiber.Database(self.dbname, self.env)
        db.ensure()
        self.env['dbname'] = self.dbname
        self.home = TempHome()
        self.machine_id = random_id()
        self.env['machine_id'] = self.machine_id
        self.env['filestore'] = {'_id': random_id(), 'path': self.home.path}

    def tearDown(self):
        self.tmpcouch.kill()
        self.tmpcouch = None
        self.home = None
        self.dbname = None
        self.env = None
