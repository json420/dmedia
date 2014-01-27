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
from random import SystemRandom
from base64 import b64decode

from degu.misc import TempSSLServer
from dbase32 import random_id
from degu.client import Client
from usercouch.misc import TempCouch
import microfiber

from dmedia import identity, rgiapps


random = SystemRandom()


def random_dbname():
    return 'db-' + random_id().lower()


def build_proxy_app(couch_env):
    return rgiapps.ProxyApp(couch_env)


def random_attachment():
    size = random.randint(1, 34969)
    data = os.urandom(size)
    return microfiber.Attachment('application/octet-stream', data)


def random_doc(i):
    """
    2/3rds of docs wont have an attachment
    """
    if i % 3 != 0:
        return {'_id': random_id()}
    att = random_attachment()
    return {
        '_id': random_id(30),
        '_attachments': {
            random_id(): microfiber.encode_attachment(att),
        },
    }



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

    def test_pull_replication(self):
        """
        Test pull replication Couch1 <= SSLServer <= Couch2.
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
        self.assertTrue(db2.ensure())

        # Save 100 docs in bulk in couch2.db2:
        docs = [random_doc(i) for i in range(100)]
        db2.save_many(docs)

        # Now save another 50 docs couch2.db2 sequentially:
        for i in range(50):
            doc = random_doc(i)
            db2.save(doc)
            docs.append(doc)

        # Add an attachment on 69 random docs:
        self.assertEqual(len(docs), 150)
        changed = random.sample(docs, 69)
        for doc in changed:
            _id = doc['_id']
            _rev = doc['_rev']
            att = random_attachment()
            doc['_rev'] = db2.put_att2(att, _id, random_id(), rev=_rev)['rev']

        # Save another 75 docs in bulk in couch2.db2:
        more_docs = [random_doc(i) for i in range(75)]
        db2.save_many(more_docs)
        docs.extend(more_docs)

        # Do sequential couch1.db1 <= SSLServer <= couch2.db2 pull replication:
        env = {'url': httpd.url}
        result = s1.pull(name1, name2, env, create_target=True)
        self.assertEqual(set(result),
            {'ok', 'history', 'session_id', 'source_last_seq', 'replication_id_version'}
        )
        self.assertIs(result['ok'], True)

        for doc in docs:
            _id = doc['_id']
            attachments = doc.pop('_attachments', None)
            saved = db1.get(_id)
            saved.pop('_attachments', None)
            self.assertEqual(doc, saved)
            if attachments:
                for (key, item) in attachments.items():
                    self.assertEqual(
                        db1.get_att(_id, key).data,
                        b64decode(item['data'].encode())
                    )

    def test_push_replication(self):
        """
        Test push replication Couch1 => SSLServer => Couch2.
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

        # Save 100 docs in bulk in couch1.db1:
        docs = [random_doc(i) for i in range(100)]
        db1.save_many(docs)

        # Now save another 50 docs couch1.db1 sequentially:
        for i in range(50):
            doc = random_doc(i)
            db1.save(doc)
            docs.append(doc)

        # Add an attachment on 69 random docs:
        self.assertEqual(len(docs), 150)
        changed = random.sample(docs, 69)
        for doc in changed:
            _id = doc['_id']
            _rev = doc['_rev']
            att = random_attachment()
            doc['_rev'] = db1.put_att2(att, _id, random_id(), rev=_rev)['rev']

        # Save another 75 docs in bulk in couch1.db1:
        more_docs = [random_doc(i) for i in range(75)]
        db1.save_many(more_docs)
        docs.extend(more_docs)

        # Do sequential couch1.db1 => SSLServer => couch2.db2 replication:
        env = {'url': httpd.url}
        result = s1.push(name1, name2, env, create_target=True)
        self.assertEqual(set(result),
            {'ok', 'history', 'session_id', 'source_last_seq', 'replication_id_version'}
        )
        self.assertIs(result['ok'], True)

        for doc in docs:
            _id = doc['_id']
            attachments = doc.pop('_attachments', None)
            saved = db2.get(_id)
            saved.pop('_attachments', None)
            self.assertEqual(doc, saved)
            if attachments:
                for (key, item) in attachments.items():
                    self.assertEqual(
                        db2.get_att(_id, key).data,
                        b64decode(item['data'].encode())
                    )

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
        docs = [random_doc(i) for i in range(100)]
        db1.save_many(docs)

        # Start couch1.db1 => SSLServer => couch2.db2 replication:
        env = {'url': httpd.url}
        result = s1.push(name1, name2, env, continuous=True, create_target=True)
        self.assertEqual(set(result), set(['_local_id', 'ok']))
        self.assertIs(result['ok'], True)

        # Now save another 50 docs couch1.db1 sequentially:
        for i in range(50):
            doc = random_doc(i)
            db1.save(doc)
            docs.append(doc)

        # Last, save another 75 docs in bulk in couch1.db1, just to be mean:
        more_docs = [random_doc(i) for i in range(75)]
        db1.save_many(more_docs)
        docs.extend(more_docs)

        time.sleep(3)
        for doc in docs:
            _id = doc['_id']
            attachments = doc.pop('_attachments', None)
            saved = db2.get(_id)
            saved.pop('_attachments', None)
            self.assertEqual(doc, saved)
            if attachments:
                for (key, item) in attachments.items():
                    self.assertEqual(
                        db2.get_att(_id, key).data,
                        b64decode(item['data'].encode())
                    )

        # Test with attachment to make sure LP:1080339 doesn't come back:
        #     https://bugs.launchpad.net/dmedia/+bug/1080339
        self.assertEqual(len(docs), 225)
        changed = random.sample(docs, 69)
        for doc in changed:
            _id = doc['_id']
            _rev = doc['_rev']
            att = random_attachment()
            doc['_rev'] = db1.put_att2(att, _id, random_id(), rev=_rev)['rev']
        time.sleep(3)
        for doc in docs:
            _id = doc['_id']
            attachments = doc.pop('_attachments', None)
            saved = db2.get(_id)
            saved.pop('_attachments', None)
            self.assertEqual(doc, saved)
            if attachments:
                for (key, item) in attachments.items():
                    self.assertEqual(
                        db2.get_att(_id, key).data,
                        b64decode(item['data'].encode())
                    )
