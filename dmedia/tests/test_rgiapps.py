# dmedia: distributed media library
# Copyright (C) 2014 Novacut Inc
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under
# the terms of the GNU Affero General Public License as published by the Free
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
#
# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>

"""
Unit tests for `dmedia.rgiapps`.
"""

from unittest import TestCase
import threading
import time
import os

from degu.misc import TempSSLServer
from dbase32 import random_id
from degu.client import Client
from usercouch.misc import TempCouch
import microfiber

from dmedia import identity, rgiapps


def random_dbname():
    return 'db-' + random_id().lower()


def build_proxy_app(couch_env):
    return rgiapps.ProxyApp(couch_env)


class TestProxyApp(TestCase):
    def test_init(self):
        password = random_id()
        env = {
            'basic': {'username': 'admin', 'password': password},
            'url': microfiber.HTTP_IPv4_URL,
        }
        app = rgiapps.ProxyApp(env)
        self.assertIsInstance(app.threadlocal, threading.local)
        self.assertEqual(app.hostname, '127.0.0.1')
        self.assertEqual(app.port, 5984)
        self.assertEqual(app.target_host, '127.0.0.1:5984')
        self.assertEqual(app.basic_auth,
            microfiber.basic_auth_header(env['basic'])
        )

    def test_get_client(self):
        password = random_id()
        env = {
            'basic': {'username': 'admin', 'password': password},
            'url': microfiber.HTTP_IPv4_URL,
        }
        app = rgiapps.ProxyApp(env)
        client = app.get_client()
        self.assertIsInstance(client, Client)
        self.assertEqual(client.hostname, '127.0.0.1')
        self.assertEqual(client.port, 5984)
        self.assertIs(client, app.threadlocal.client)
        self.assertIs(app.get_client(), client)

    def test_push_replication(self):
        pki = identity.TempPKI(True)
        config = {'replicator': pki.get_client_config()}
        couch1 = TempCouch()
        couch2 = TempCouch()

        # couch1 needs the replication SSL config
        env1 = couch1.bootstrap('basic', config)
        s1 = microfiber.Server(env1)

        # couch2 needs env['user_id']
        env2 = couch2.bootstrap('basic', None)
        env2['user_id'] = pki.client_ca.id
        env2['machine_id'] = pki.server.id
        s2 = microfiber.Server(env2)

        # httpd is the SSL frontend for couch2
        httpd = TempSSLServer(pki, build_proxy_app, env2)

        # Create just the source DB, rely on create_target=True for remote
        name1 = random_dbname()
        name2 = random_dbname()
        self.assertEqual(s1.put(None, name1), {'ok': True})

        env = {'url': 'https://[::1]:{}/'.format(httpd.port)}
        result = s1.push(name1, name2, env, continuous=True, create_target=True)
        self.assertEqual(set(result), set(['_local_id', 'ok']))
        self.assertIs(result['ok'], True)

        # Save docs in s1.name1, make sure they show up in s2.name2
        docs = [{'_id': random_id()} for i in range(100)]
        for doc in docs:
            doc['_rev'] = s1.post(doc, name1)['rev']
        time.sleep(1)
        for doc in docs:
            self.assertEqual(s2.get(name2, doc['_id']), doc)

        # Test with attachment to make sure LP:1080339 doesn't come back:
        #     https://bugs.launchpad.net/dmedia/+bug/1080339
        _id = random_id()
        data = os.urandom(68400)
        doc = {'_id': _id}
        doc['_rev'] = s1.post(doc, name1)['rev']
        time.sleep(2)
        thumbnail = microfiber.Attachment('image/jpeg', data)
        doc['_attachments'] = {
            'thumbnail': microfiber.encode_attachment(thumbnail),
        }
        s1.post(doc, name1)
        time.sleep(2)
        att = s2.get_att(name2, _id, 'thumbnail')
        self.assertEqual(att.content_type, 'image/jpeg')
        self.assertEqual(att.data, data)

    def test_continuous_push_replication(self):
        """
        Test *continuous* push replication Couch1 => SSLServer => Couch2.
        """
        pki = identity.TempPKI(True)
        config = {'replicator': pki.get_client_config()}
        couch1 = TempCouch()
        couch2 = TempCouch()

        # couch1 needs the replication SSL config
        env1 = couch1.bootstrap('basic', config)
        s1 = microfiber.Server(env1)

        # couch2 needs env['user_id']
        env2 = couch2.bootstrap('basic', None)
        env2['user_id'] = pki.client_ca.id
        env2['machine_id'] = pki.server.id
        s2 = microfiber.Server(env2)

        # httpd is the SSL frontend for couch2
        httpd = TempSSLServer(pki, build_proxy_app, env2)

        # Create just the source DB, rely on create_target=True for remote
        name1 = random_dbname()
        name2 = random_dbname()
        db1 = s1.database(name1)
        db2 = s2.database(name2)
        self.assertTrue(db1.ensure())

        # Save 100 docs in couch1.db1 *before* we start the replication:
        docs = [{'_id': random_id()} for i in range(100)]
        db1.save_many(docs)

        # Start couch1.db1 => SSLServer => couch2.db2 replication:
        env = {'url': httpd.url}
        result = s1.push(name1, name2, env, continuous=True, create_target=True)
        self.assertEqual(set(result), set(['_local_id', 'ok']))
        self.assertIs(result['ok'], True)

        # Now save another 50 docs couch1.db1 sequentially:
        for i in range(50):
            doc = {'_id': random_id()}
            db1.save(doc)
            docs.append(doc)

        # Last, save another 75 docs in bulk in couch1.db1, just to be mean:
        more_docs = [{'_id': random_id()} for i in range(75)]
        db1.save_many(more_docs)
        docs.extend(more_docs)

        time.sleep(3)
        for doc in docs:
            self.assertEqual(s2.get(name2, doc['_id']), doc)
        return

        # Test with attachment to make sure LP:1080339 doesn't come back:
        #     https://bugs.launchpad.net/dmedia/+bug/1080339
        _id = random_id()
        data = os.urandom(68400)
        doc = {'_id': _id}
        doc['_rev'] = s1.post(doc, name1)['rev']
        time.sleep(2)
        thumbnail = microfiber.Attachment('image/jpeg', data)
        doc['_attachments'] = {
            'thumbnail': microfiber.encode_attachment(thumbnail),
        }
        s1.post(doc, name1)
        time.sleep(2)
        att = s2.get_att(name2, _id, 'thumbnail')
        self.assertEqual(att.content_type, 'image/jpeg')
        self.assertEqual(att.data, data)
