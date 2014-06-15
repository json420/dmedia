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
import os
from random import SystemRandom
from base64 import b64decode
import socket
import json

from dbase32 import random_id
from degu import IPv6_LOOPBACK, IPv4_LOOPBACK
from degu.misc import TempServer, TempSSLServer, TempPKI
from degu.base import EmptyPreambleError
from degu.client import Client, SSLClient, build_client_sslctx
from usercouch.misc import TempCouch
import microfiber
from microfiber import Attachment, encode_attachment
from microfiber import replicator

import dmedia
from dmedia.local import LocalSlave
from dmedia import client, rgiapps


random = SystemRandom()


def random_dbname():
    return 'db-' + random_id().lower()


def build_proxy_app(couch_env):
    return rgiapps.ProxyApp(couch_env)


def random_attachment():
    size = random.randint(1, 34969)
    data = os.urandom(size)
    return Attachment('application/octet-stream', data)


def random_doc(i):
    """
    1/3rd of docs will have an attachment.
    """
    doc = {
        '_id': random_id(30),
        '_attachments': {},
        'i': i,
    }
    if i % 3 == 0:
        att = random_attachment()
        doc['_attachments'][random_id()] = encode_attachment(att)
    return doc


class TestFunctions(TestCase):
    def test_range_to_slice(self):
        # First byte:
        self.assertEqual(
            rgiapps.range_to_slice('bytes=0-0', 1000), (0, 1)
        )

        # Final byte:
        self.assertEqual(
            rgiapps.range_to_slice('bytes=999-999', 1000), (999, 1000)
        )

        # stop > file_size
        with self.assertRaises(rgiapps.RGIError) as cm:
            rgiapps.range_to_slice('bytes=999-1000', 1000)
        self.assertEqual(
            str(cm.exception),
            '416 Requested Range Not Satisfiable'
        )

        # start >= stop
        with self.assertRaises(rgiapps.RGIError) as cm:
            rgiapps.range_to_slice('bytes=200-199', 1000)
        self.assertEqual(
            str(cm.exception),
            '416 Requested Range Not Satisfiable'
        )

        # But confirm this works:
        self.assertEqual(
            rgiapps.range_to_slice('bytes=200-200', 1000), (200, 201)
        )

        # And this too:
        self.assertEqual(
            rgiapps.range_to_slice('bytes=100-199', 1000), (100, 200)
        )

        # Use above as basis for a bunch of malformed values:
        bad_eggs = [
            ' bytes=100-199',
            'bytes=100-199 ',
            'bytes= 100-199',
            'bytes=-100',
            'bytes=100-',
            'bytes=-100-199',
            'bits=100-199',
            'cows=100-199',
            'bytes=10.0-20.0',
            'bytes=100:199'
        ]
        for value in bad_eggs:
            with self.assertRaises(rgiapps.RGIError) as cm:
                rgiapps.range_to_slice(value, 1000)
            self.assertEqual(str(cm.exception), '400 Bad Range Request')

       # Test the round-trip with client.bytes_range
        slices = [
            (0, 1),
            (0, 500),
            (500, 1000),
            (9500, 10000),
        ]
        for (start, stop) in slices:
            self.assertEqual(
                rgiapps.range_to_slice(client.bytes_range(start, stop), 10000),
                (start, stop)
            )


class TestRootApp(TestCase):
    def test_init(self):
        user_id = random_id(30)
        machine_id = random_id(30)
        password = random_id()
        env = {
            'user_id': user_id,
            'machine_id': machine_id,
            'basic': {'username': 'admin', 'password': password},
            'url': microfiber.HTTP_IPv4_URL,
        }
        app = rgiapps.RootApp(env)
        self.assertIs(app.user_id, user_id)
        self.assertEqual(
            app.info,
            microfiber.dumps(
                {
                    'user_id': user_id,
                    'machine_id': machine_id,
                    'version': dmedia.__version__,
                    'user': os.environ.get('USER'),
                    'host': socket.gethostname(),
                }   
            ).encode('utf-8')
        )
        self.assertEqual(app.info_length, int(len(app.info)))
        self.assertIsInstance(app.proxy, rgiapps.ProxyApp)
        self.assertIsInstance(app.files, rgiapps.FilesApp)
        self.assertEqual(app.map,
            {
                '': app.get_info,
                'couch': app.proxy,
                'files': app.files,
            }
        )

    def test_call(self):
        user_id = random_id(30)
        machine_id = random_id(30)
        password = random_id()
        env = {
            'user_id': user_id,
            'machine_id': machine_id,
            'basic': {'username': 'admin', 'password': password},
            'url': microfiber.HTTP_IPv4_URL,
        }
        app = rgiapps.RootApp(env)
        self.assertEqual(
            app({}, {'path': [], 'method': 'GET'}),
            (
                200,
                'OK',
                {
                    'content-length': len(app.info),
                    'content-type': 'application/json',
                },
                app.info
            )
        )
        self.assertEqual(
            app({}, {'path': [''], 'method': 'GET'}),
            (
                200,
                'OK',
                {
                    'content-length': len(app.info),
                    'content-type': 'application/json',
                },
                app.info
            )
        )
        self.assertEqual(
            app({}, {'path': [], 'method': 'POST'}),
            (405, 'Method Not Allowed', {}, None)
        )
        self.assertEqual(
            app({}, {'path': [''], 'method': 'POST'}),
            (405, 'Method Not Allowed', {}, None)
        )
        request = {'script': [], 'path': ['foo'], 'method': 'POST'}
        self.assertEqual(app({}, request), (410, 'Gone', {}, None))
        self.assertEqual(request,
            {'script': ['foo'], 'path': [], 'method': 'POST'}
        )
        request = {'script': [], 'path': ['foo', 'bar'], 'method': 'GET'}
        self.assertEqual(app({}, request), (410, 'Gone', {}, None))
        self.assertEqual(request,
            {'script': ['foo'], 'path': ['bar'], 'method': 'GET'}
        )

    def test_on_connect(self):
        user_id = random_id(30)
        machine_id = random_id(30)
        password = random_id()
        env = {
            'user_id': user_id,
            'machine_id': machine_id,
            'basic': {'username': 'admin', 'password': password},
            'url': microfiber.HTTP_IPv4_URL,
        }
        app = rgiapps.RootApp(env)
        # Test with non ssl.SSLSocket:
        for family in (socket.AF_INET6, socket.AF_INET, socket.AF_UNIX):
            sock = socket.socket(family, socket.SOCK_STREAM)
            environ = {'client': random_id()}
            self.assertIs(app.on_connect(sock, environ), False)

    def test_get_info(self):
        user_id = random_id(30)
        machine_id = random_id(30)
        password = random_id()
        env = {
            'user_id': user_id,
            'machine_id': machine_id,
            'basic': {'username': 'admin', 'password': password},
            'url': microfiber.HTTP_IPv4_URL,
        }
        app = rgiapps.RootApp(env)
        self.assertEqual(
            app.get_info({}, {'method': 'GET'}),
            (
                200,
                'OK',
                {
                    'content-length': len(app.info),
                    'content-type': 'application/json',
                },
                app.info
            )
        )
        self.assertEqual(
            app.get_info({}, {'method': 'POST'}),
            (405, 'Method Not Allowed', {}, None)
        )


class TestRootAppLive(TestCase):
    def test_call(self):
        couch = TempCouch()
        env = couch.bootstrap()
        env['user_id'] = random_id(30)
        env['machine_id'] = random_id(30)
        db = microfiber.Database('dmedia-1', env)

        # Security critical: ensure that RootApp.on_connect() prevents
        # misconfiguration, wont accept connections without SSL:
        httpd = TempServer(IPv4_LOOPBACK, rgiapps.build_root_app, env)
        client = Client(httpd.address)
        conn = client.connect()
        with self.assertRaises(EmptyPreambleError):
            conn.request('GET', '/')
 
        # Security critical: ensure that RootApp.on_connect() prevents
        # misconfiguration, wont accept connections when anonymous client access
        # is allowed:
        pki = TempPKI()
        server_config = pki.get_anonymous_server_config()
        client_config = pki.get_anonymous_client_config()
        httpd = TempSSLServer(
            server_config, IPv4_LOOPBACK, rgiapps.build_root_app, env
        )
        client = SSLClient(build_client_sslctx(client_config), httpd.address)
        conn = client.connect()
        with self.assertRaises(EmptyPreambleError):
            conn.request('GET', '/')

        # Now setup a proper SSLServer:
        httpd = TempSSLServer(
            pki.get_server_config(), IPv4_LOOPBACK, rgiapps.build_root_app, env
        )
        sslctx = build_client_sslctx(pki.get_client_config())
        client = SSLClient(sslctx, httpd.address)

        # Info app
        conn = client.connect()
        response = conn.request('GET', '/')
        data = response.body.read()
        self.assertEqual(json.loads(data.decode()), {
            'user_id': env['user_id'],
            'machine_id': env['machine_id'],
            'version': dmedia.__version__,
            'user': os.environ.get('USER'),
            'host': socket.gethostname(),
        })
        self.assertEqual(response.status, 200)
        self.assertEqual(response.reason, 'OK')
        self.assertEqual(response.headers, {
            'content-length': len(data),
            'content-type': 'application/json',
        })

        # Naughty path in ProxyAPP
        self.assertEqual(conn.request('GET', '/couch/_config'),
            (403, 'Forbidden', {}, None)
        )

        # Ensure that server closed the connection:
        with self.assertRaises(EmptyPreambleError):
            conn.request('GET', '/couch/')
        self.assertIs(conn.closed, True)
        self.assertIsNone(conn.response_body)
        self.assertIsNone(conn.sock)

        # A 404 should not close the connection:
        conn = client.connect()
        response = conn.request('GET', '/couch/dmedia-1')
        data = response.body.read()
        self.assertEqual(response.status, 404)
        self.assertEqual(response.reason, 'Object Not Found')
        self.assertEqual(response.headers['content-length'], len(data))
        db.put(None)
        response = conn.request('GET', '/couch/dmedia-1')
        data = response.body.read()
        self.assertEqual(response.status, 200)
        self.assertEqual(response.reason, 'OK')
        self.assertEqual(response.headers['content-length'], len(data))
        self.assertEqual(db.get(), json.loads(data.decode()))


class TestProxyApp(TestCase):
    def test_init(self):
        password = random_id()
        env = {
            'basic': {'username': 'admin', 'password': password},
            'url': microfiber.HTTP_IPv4_URL,
        }
        app = rgiapps.ProxyApp(env)
        self.assertIsInstance(app.client, Client)
        self.assertEqual(app.client.address, ('127.0.0.1', 5984))
        self.assertEqual(app.client.base_headers, {
            'authorization': microfiber.basic_auth_header(env['basic']),
            'host': '127.0.0.1:5984',
        })

    def test_push_proxy_dst(self):
        """
        Test couch_A => (SSLServer => couch_B).

        In a nutshell, this makes sure the Microfiber replicator works when the
        *source* is a normal CouchDB instance and the *destination* is a Degu
        SSLServer reverse proxy running the Dmedia ProxyApp.

        The assumption here is that the replicator is running on the source
        node, which in the case of Dmedia it must be as we don't allow CouchDB
        to directly accept connections from the outside world.  So in terms of
        the CouchDB replicator nomenclature, this would be "push" replication.

        The subtle difference is that the actual CouchDB replicator will always
        be running as part of the CouchDB process at one of the endpoints,
        whereas the Microfiber replicator is a completely independent process
        that could be running anywhere, even on a 3rd machine.
        """
        pki = TempPKI()
        couch_A = TempCouch()
        couch_B = TempCouch()
        env_A = couch_A.bootstrap()
        env_B = couch_B.bootstrap()
        uuid_A = random_id()
        uuid_B = random_id()

        # httpd is the SSL frontend for couch_B:
        # (for more fun, CouchDB instances are IPv4, SSLServer is IPv6)
        httpd = TempSSLServer(
            pki.get_server_config(), IPv6_LOOPBACK, build_proxy_app, env_B
        )
        env_proxy_B = {
            'url': httpd.url,
            'ssl': pki.get_client_config(),
        }

        # Create just the source DB, microfiber.replicator should create the dst
        # (even through the proxy):
        name_A = random_dbname()
        name_B = random_dbname()
        db_A = microfiber.Database(name_A, env_A)
        self.assertTrue(db_A.ensure())
        db_B = microfiber.Database(name_B, env_B)
        db_proxy_B = microfiber.Database(name_B, env_proxy_B)

        # Save 100 docs in bulk in db_A (the source):
        docs = [random_doc(i) for i in range(100)]
        db_A.save_many(docs)

        # Now save another 50 docs db_A sequentially:
        for i in range(50):
            doc = random_doc(i)
            db_A.save(doc)
            docs.append(doc)

        # Add an attachment on 69 random docs:
        self.assertEqual(len(docs), 150)
        changed = random.sample(docs, 69)
        for doc in changed:
            att = random_attachment()
            doc['_attachments'][random_id()] = encode_attachment(att)
            db_A.save(doc)

        # Save another 75 docs in bulk in db_A:
        more_docs = [random_doc(i) for i in range(75)]
        db_A.save_many(more_docs)
        docs.extend(more_docs)

        # Replicate db_A => db_proxy_B:
        session = replicator.load_session(uuid_A, db_A, uuid_B, db_proxy_B)
        self.assertNotIn('update_seq', session)
        self.assertEqual(session['doc_count'], 0)
        replicator.replicate(session)
        self.assertEqual(session['update_seq'], 294)  # 69 docs at rev 2-*
        self.assertEqual(session['doc_count'], 225)

        # Verify results in db_B:
        for doc in docs:
            _id = doc['_id']
            attachments = doc.pop('_attachments', None)
            saved = db_B.get(_id)
            saved.pop('_attachments', None)
            self.assertEqual(doc, saved)
            if attachments:
                for (key, item) in attachments.items():
                    self.assertEqual(
                        db_B.get_att(_id, key).data,
                        b64decode(item['data'].encode())
                    )

        # Make sure replication session can be resumed, checkpoint was done:
        ids = [d['_id'] for d in docs]
        docs = db_A.get_many(ids)
        session = replicator.load_session(uuid_A, db_A, uuid_B, db_proxy_B)
        self.assertEqual(session['update_seq'], 294)  # 69 docs at rev 2-*
        self.assertEqual(session['doc_count'], 0)
        replicator.replicate(session)
        self.assertEqual(session['update_seq'], 294)  # 69 docs at rev 2-*
        self.assertEqual(session['doc_count'], 0)
        self.assertEqual(db_B.get_many(ids), docs)
        self.assertEqual(db_A.get_many(ids), docs)

        # Make a change to the first 18 docs:
        for doc in docs[:18]:
            doc['changed'] = True
            db_A.save(doc)

        # Test replication resume when there are at least some changes:
        session = replicator.load_session(uuid_A, db_A, uuid_B, db_proxy_B)
        self.assertEqual(session['update_seq'], 294)
        self.assertEqual(session['doc_count'], 0)
        replicator.replicate(session)
        self.assertEqual(session['update_seq'], 312)  # 225 + 69 + 18
        self.assertEqual(session['doc_count'], 18)
        self.assertEqual(db_B.get_many(ids), docs)
        self.assertEqual(db_A.get_many(ids), docs)

        # Once more with feeling:
        session = replicator.load_session(uuid_A, db_A, uuid_B, db_proxy_B)
        self.assertEqual(session['update_seq'], 312)
        self.assertEqual(session['doc_count'], 0)
        replicator.replicate(session)
        self.assertEqual(session['update_seq'], 312)
        self.assertEqual(session['doc_count'], 0)
        self.assertEqual(db_B.get_many(ids), docs)
        self.assertEqual(db_A.get_many(ids), docs)

        # Oh, now screw up the session:
        session = replicator.load_session(random_id(), db_A, uuid_B, db_proxy_B)
        self.assertNotIn('update_seq', session)
        self.assertEqual(session['doc_count'], 0)
        replicator.replicate(session)
        self.assertEqual(session['update_seq'], 312)
        self.assertEqual(session['doc_count'], 0)
        self.assertEqual(db_B.get_many(ids), docs)
        self.assertEqual(db_A.get_many(ids), docs)

        # Make a change to the first 21 docs:
        for doc in docs[:21]:
            doc['changed'] = True
            db_A.save(doc)

        # Screw up the session again, but this time when there are changes:
        session = replicator.load_session(uuid_A, db_A, random_id(), db_proxy_B)
        self.assertNotIn('update_seq', session)
        self.assertEqual(session['doc_count'], 0)
        replicator.replicate(session)
        self.assertEqual(session['update_seq'], 333)  # 225 + 69 + 18 + 21
        self.assertEqual(session['doc_count'], 21)
        self.assertEqual(db_B.get_many(ids), docs)
        self.assertEqual(db_A.get_many(ids), docs)

        # Yet once more with yet more feeling (and a screwed up session):
        session = replicator.load_session('foo', db_A, 'bar', db_proxy_B)
        self.assertNotIn('update_seq', session)
        self.assertEqual(session['doc_count'], 0)
        replicator.replicate(session)
        self.assertEqual(session['update_seq'], 333)
        self.assertEqual(session['doc_count'], 0)
        self.assertEqual(db_B.get_many(ids), docs)
        self.assertEqual(db_proxy_B.get_many(ids), docs)
        self.assertEqual(db_A.get_many(ids), docs)

    def test_pull_proxy_src(self):
        """
        Test (couch_A => SSLServer) => couch_B.

        In a nutshell, this makes sure the Microfiber replicator works when the
        *source* is a Degu SSLServer reverse proxy running the Dmedia ProxyApp,
        and the *destination* is a normal CouchDB instance.

        The assumption here is that the replicator is running on the destination
        node, which in the case of Dmedia it must be as we don't allow CouchDB
        to directly accept connections from the outside world.  So in terms of
        the CouchDB replicator nomenclature, this would be "pull" replication.

        The subtle difference is that the actual CouchDB replicator will always
        be running as part of the CouchDB process at one of the endpoints,
        whereas the Microfiber replicator is a completely independent process
        that could be running anywhere, even on a 3rd machine.
        """
        pki = TempPKI()
        couch_A = TempCouch()
        couch_B = TempCouch()
        env_A = couch_A.bootstrap()
        env_B = couch_B.bootstrap()
        uuid_A = random_id()
        uuid_B = random_id()

        # httpd is the SSL frontend for couch_A:
        # (for more fun, CouchDB instances are IPv4, SSLServer is IPv6)
        httpd = TempSSLServer(
            pki.get_server_config(), IPv6_LOOPBACK, build_proxy_app, env_A
        )
        env_proxy_A = {
            'url': httpd.url,
            'ssl': pki.get_client_config(),
        }

        # Create just the source DB, microfiber.replicator should create the dst:
        name_A = random_dbname()
        name_B = random_dbname()
        db_A = microfiber.Database(name_A, env_A)
        self.assertTrue(db_A.ensure())
        db_proxy_A = microfiber.Database(name_A, env_proxy_A)
        db_B = microfiber.Database(name_B, env_B)

        # Save 100 docs in bulk in db_A (the source):
        docs = [random_doc(i) for i in range(100)]
        db_A.save_many(docs)

        # Now save another 50 docs db_A sequentially:
        for i in range(50):
            doc = random_doc(i)
            db_A.save(doc)
            docs.append(doc)

        # Add an attachment on 69 random docs:
        self.assertEqual(len(docs), 150)
        changed = random.sample(docs, 69)
        for doc in changed:
            att = random_attachment()
            doc['_attachments'][random_id()] = encode_attachment(att)
            db_A.save(doc)

        # Save another 75 docs in bulk in db_A:
        more_docs = [random_doc(i) for i in range(75)]
        db_A.save_many(more_docs)
        docs.extend(more_docs)

        # Replicate db_proxy_A => db_B:
        session = replicator.load_session(uuid_A, db_proxy_A, uuid_B, db_B)
        self.assertNotIn('update_seq', session)
        self.assertEqual(session['doc_count'], 0)
        replicator.replicate(session)
        self.assertEqual(session['update_seq'], 294)  # 69 docs at rev 2-*
        self.assertEqual(session['doc_count'], 225)

        # Verify results in db_B:
        for doc in docs:
            _id = doc['_id']
            attachments = doc.pop('_attachments', None)
            saved = db_B.get(_id)
            saved.pop('_attachments', None)
            self.assertEqual(doc, saved)
            if attachments:
                for (key, item) in attachments.items():
                    self.assertEqual(
                        db_B.get_att(_id, key).data,
                        b64decode(item['data'].encode())
                    )

        # Make sure replication session can be resumed, checkpoint was done:
        ids = [d['_id'] for d in docs]
        docs = db_A.get_many(ids)
        session = replicator.load_session(uuid_A, db_proxy_A, uuid_B, db_B)
        self.assertEqual(session['update_seq'], 294)  # 69 docs at rev 2-*
        self.assertEqual(session['doc_count'], 0)
        replicator.replicate(session)
        self.assertEqual(session['update_seq'], 294)  # 69 docs at rev 2-*
        self.assertEqual(session['doc_count'], 0)
        self.assertEqual(db_B.get_many(ids), docs)
        self.assertEqual(db_A.get_many(ids), docs)

        # Make a change to the first 18 docs:
        for doc in docs[:18]:
            doc['changed'] = True
            db_A.save(doc)

        # Test replication resume when there are at least some changes:
        session = replicator.load_session(uuid_A, db_proxy_A, uuid_B, db_B)
        self.assertEqual(session['update_seq'], 294)
        self.assertEqual(session['doc_count'], 0)
        replicator.replicate(session)
        self.assertEqual(session['update_seq'], 312)  # 225 + 69 + 18
        self.assertEqual(session['doc_count'], 18)
        self.assertEqual(db_B.get_many(ids), docs)
        self.assertEqual(db_A.get_many(ids), docs)

        # Once more with feeling:
        session = replicator.load_session(uuid_A, db_proxy_A, uuid_B, db_B)
        self.assertEqual(session['update_seq'], 312)
        self.assertEqual(session['doc_count'], 0)
        replicator.replicate(session)
        self.assertEqual(session['update_seq'], 312)
        self.assertEqual(session['doc_count'], 0)
        self.assertEqual(db_B.get_many(ids), docs)
        self.assertEqual(db_A.get_many(ids), docs)

        # Oh, now screw up the session:
        session = replicator.load_session(random_id(), db_proxy_A, uuid_B, db_B)
        self.assertNotIn('update_seq', session)
        self.assertEqual(session['doc_count'], 0)
        replicator.replicate(session)
        self.assertEqual(session['update_seq'], 312)
        self.assertEqual(session['doc_count'], 0)
        self.assertEqual(db_B.get_many(ids), docs)
        self.assertEqual(db_A.get_many(ids), docs)

        # Make a change to the first 21 docs:
        for doc in docs[:21]:
            doc['changed'] = True
            db_A.save(doc)

        # Screw up the session again, but this time when there are changes:
        session = replicator.load_session(uuid_A, db_proxy_A, random_id(), db_B)
        self.assertNotIn('update_seq', session)
        self.assertEqual(session['doc_count'], 0)
        replicator.replicate(session)
        self.assertEqual(session['update_seq'], 333)  # 225 + 69 + 18 + 21
        self.assertEqual(session['doc_count'], 21)
        self.assertEqual(db_B.get_many(ids), docs)
        self.assertEqual(db_A.get_many(ids), docs)

        # Yet once more with yet more feeling (and a screwed up session):
        session = replicator.load_session('foo', db_proxy_A, 'bar', db_B)
        self.assertNotIn('update_seq', session)
        self.assertEqual(session['doc_count'], 0)
        replicator.replicate(session)
        self.assertEqual(session['update_seq'], 333)
        self.assertEqual(session['doc_count'], 0)
        self.assertEqual(db_B.get_many(ids), docs)
        self.assertEqual(db_A.get_many(ids), docs)
        self.assertEqual(db_proxy_A.get_many(ids), docs)


class TestFilesApp(TestCase):
    def test_init(self):
        password = random_id()
        env = {
            'basic': {'username': 'admin', 'password': password},
            'url': microfiber.HTTP_IPv4_URL,
            'machine_id': random_id(30),
        }
        app = rgiapps.FilesApp(env)
        self.assertIsInstance(app.local, LocalSlave)
        self.assertIs(app.local.db.env, env)

    def test_call(self):
        password = random_id()
        env = {
            'basic': {'username': 'admin', 'password': password},
            'url': microfiber.HTTP_IPv4_URL,
            'machine_id': random_id(30),
        }
        app = rgiapps.FilesApp(env)

        # method:
        for method in ('PUT', 'POST', 'DELETE'):
            self.assertEqual(app({}, {'method': method}),
                (405, 'Method Not Allowed', {}, None)
            )

        # path:
        bad_id1 = random_id(30)[:-1] + '0'  # Invalid letter
        request = {'method': 'GET', 'script': [], 'path': [bad_id1]}
        self.assertEqual(app({}, request), (400, 'Bad File ID', {}, None))
        self.assertEqual(request,
            {'method': 'GET', 'script': [bad_id1], 'path': []}
        )

        bad_id2 = random_id(25)  # Wrong length
        request = {'method': 'GET', 'script': [], 'path': [bad_id2]}
        self.assertEqual(app({}, request), (400, 'Bad File ID Length', {}, None))
        self.assertEqual(request,
            {'method': 'GET', 'script': [bad_id2], 'path': []}
        )

        good_id = random_id(30)
        request = {'method': 'GET', 'script': [], 'path': [good_id, 'more']}
        self.assertEqual(app({}, request), (410, 'Gone', {}, None))
        self.assertEqual(request,
            {'method': 'GET', 'script': [good_id], 'path': ['more']}
        )

        # query:
        request = {
            'method': 'GET',
            'script': [],
            'path': [good_id],
            'query': 'stuff=junk',
        }
        self.assertEqual(app({}, request), (400, 'No Query For You', {}, None))
        self.assertEqual(request, {
            'method': 'GET',
            'script': [good_id],
            'path': [],
            'query': 'stuff=junk',
        })

        # HEAD + range == bad:
        request = {
            'method': 'HEAD',
            'script': [],
            'path': [good_id],
            'query': '',
            'headers': {'range': 'bytes=500-1000'},
        }
        self.assertEqual(app({}, request), (400, 'Cannot Range with HEAD', {}, None))
        self.assertEqual(request, {
            'method': 'HEAD',
            'script': [good_id],
            'path': [],
            'query': '',
            'headers': {'range': 'bytes=500-1000'},
        })

