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
from degu.base import bodies as default_bodies
from degu.misc import TempServer, TempSSLServer, TempPKI
from degu.client import Client, SSLClient
from usercouch.misc import TempCouch
import microfiber
from microfiber import Attachment, encode_attachment
from microfiber import replicator
from filestore import Leaf, Hasher
from filestore.misc import TempFileStore

from .test_metastore import create_random_file
import dmedia
from dmedia.local import LocalSlave
from dmedia import client, util, rgiapps


random = SystemRandom()


def random_dbname():
    return 'db-' + random_id().lower()


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


def address_to_url(scheme, address):
    """
    Convert `Server.address` into a URL.

    For example:

    >>> address_to_url('https', ('::1', 54321, 0, 0))
    'https://[::1]:54321/'

    >>> address_to_url('http', ('127.0.0.1', 54321))
    'http://127.0.0.1:54321/'

    """
    assert scheme in ('http', 'https')
    if isinstance(address, (str, bytes)):
        return None
    assert isinstance(address, tuple)
    assert len(address) in {4, 2}
    if len(address) == 2:  # IPv4?
        return '{}://{}:{:d}/'.format(scheme, address[0], address[1])
    # More better, IPv6:
    return '{}://[{}]:{}/'.format(scheme, address[0], address[1])


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

        # Test when random session marker is missing:
        with self.assertRaises(Exception) as cm:
            app({}, {'path': [], 'method': 'GET'}, default_bodies)
        self.assertEqual(str(cm.exception),
            'session marker None != {!r}, on_connect() was not called'.format(
                app._marker
            )
        )

        # Remaining test use a valid session marker:
        session = {'_marker': app._marker}
        self.assertEqual(
            app(session, {'path': [], 'method': 'GET'}, default_bodies),
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
            app(session, {'path': [''], 'method': 'GET'}, default_bodies),
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
            app(session, {'path': [], 'method': 'POST'}, default_bodies),
            (405, 'Method Not Allowed', {}, None)
        )
        self.assertEqual(
            app(session, {'path': [''], 'method': 'POST'}, default_bodies),
            (405, 'Method Not Allowed', {}, None)
        )
        request = {'script': [], 'path': ['foo'], 'method': 'POST'}
        self.assertEqual(app(session, request, default_bodies),
            (410, 'Gone', {}, None)
        )
        self.assertEqual(request,
            {'script': ['foo'], 'path': [], 'method': 'POST'}
        )
        request = {'script': [], 'path': ['foo', 'bar'], 'method': 'GET'}
        self.assertEqual(app(session, request, default_bodies),
            (410, 'Gone', {}, None)
        )
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
            session = {'client': random_id()}
            sock = socket.socket(family, socket.SOCK_STREAM)
            self.assertIs(app.on_connect(session, sock), False)
            self.assertNotIn('_marker', session)

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
            app.get_info({}, {'method': 'GET'}, default_bodies),
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
            app.get_info({}, {'method': 'POST'}, default_bodies),
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
        httpd = TempServer(IPv4_LOOPBACK, rgiapps.RootApp(env))
        client = Client(httpd.address)
        conn = client.connect()
        with self.assertRaises(ConnectionError):
            conn.request('GET', '/', {}, None)
 
        # Security critical: ensure that RootApp.on_connect() prevents
        # misconfiguration, wont accept connections when anonymous client access
        # is allowed:
        pki = TempPKI()
        httpd = TempSSLServer(
            pki.anonymous_server_sslconfig, IPv4_LOOPBACK, rgiapps.RootApp(env)
        )
        client = SSLClient(pki.anonymous_client_sslconfig, httpd.address)
        conn = client.connect()
        with self.assertRaises(ConnectionError):
            conn.request('GET', '/', {}, None)

        # Now setup a proper SSLServer:
        httpd = TempSSLServer(
            pki.server_sslconfig, IPv4_LOOPBACK, rgiapps.RootApp(env)
        )
        client = SSLClient(pki.client_sslconfig, httpd.address)

        # Info app
        conn = client.connect()
        response = conn.request('GET', '/', {}, None)
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
        self.assertEqual(conn.request('GET', '/couch/_config', {}, None),
            (403, 'Forbidden', {}, None)
        )

        # Ensure that server closed the connection:
        with self.assertRaises(ConnectionError):
            conn.request('GET', '/couch/', {}, None)
        self.assertIs(conn.closed, True)
        self.assertIsNone(conn._response_body)
        self.assertIsNone(conn.sock)

        # A 404 should not close the connection:
        conn = client.connect()
        response = conn.request('GET', '/couch/dmedia-1', {}, None)
        data = response.body.read()
        self.assertEqual(response.status, 404)
        self.assertEqual(response.reason, 'Object Not Found')
        self.assertEqual(response.headers['content-length'], len(data))
        db.put(None)
        response = conn.request('GET', '/couch/dmedia-1', {}, None)
        data = response.body.read()
        self.assertEqual(response.status, 200)
        self.assertEqual(response.reason, 'OK')
        self.assertEqual(response.headers['content-length'], len(data))
        self.assertEqual(db.get(), json.loads(data.decode()))

    def test_files(self):
        """
        Full-stack live test of FilesApp, through RootApp.
        """
        couch = TempCouch()
        env = couch.bootstrap()
        env['user_id'] = random_id(30)
        env['machine_id'] = random_id(30)
        db = util.get_db(env, True)
        machine = {'_id': env['machine_id'], 'stores': {}}
        db.save(machine)

        pki = TempPKI()
        httpd = TempSSLServer(
            pki.server_sslconfig, IPv4_LOOPBACK, rgiapps.RootApp(env)
        )
        client = SSLClient(pki.client_sslconfig, httpd.address)

        # Non-existent file:
        file_id = random_id(30)
        uri = '/files/{}'.format(file_id)
        conn = client.connect()
        response = conn.request('GET', uri, {}, None)
        self.assertEqual(response.status, 404)
        self.assertEqual(response.reason, 'Not Found')
        self.assertEqual(response.headers, {})
        self.assertIsNone(response.body)
        self.assertIs(conn.closed, False)

        # Same, but when there is at least a FileStore:
        fs1 = TempFileStore()
        machine['stores'][fs1.id] = {'parentdir': fs1.parentdir}
        db.save(machine)
        response = conn.request('GET', uri, {}, None)
        self.assertEqual(response.status, 404)
        self.assertEqual(response.reason, 'Not Found')
        self.assertEqual(response.headers, {})
        self.assertIsNone(response.body)
        self.assertIs(conn.closed, False)
        del conn

        # Add a file to fs1:
        doc1 = create_random_file(fs1, db)
        conn = client.connect()
        response = conn.request('GET', '/files/{}'.format(doc1['_id']), {}, None)
        self.assertEqual(response.status, 200)
        self.assertEqual(response.reason, 'OK')
        self.assertEqual(response.headers, {'content-length': doc1['bytes']})
        self.assertIs(response.body.chunked, False)
        h = Hasher()
        index = 0
        while True:
            data = response.body.read(h.protocol.leaf_size)
            if not data:
                break
            leaf = Leaf(index, data)
            h.hash_leaf(leaf)
            index += 1
        ch1 = h.content_hash()
        self.assertEqual(ch1.id, doc1['_id'])
        self.assertEqual(ch1.file_size, doc1['bytes'])
        del conn

        # Add a 2nd FileStore, again request non-existent file:
        fs2 = TempFileStore()
        machine['stores'][fs2.id] = {'parentdir': fs2.parentdir}
        db.save(machine)
        conn = client.connect()
        response = conn.request('GET', uri, {}, None)
        self.assertEqual(response.status, 404)
        self.assertEqual(response.reason, 'Not Found')
        self.assertEqual(response.headers, {})
        self.assertIsNone(response.body)
        self.assertIs(conn.closed, False)
        del conn

        # Add random file to fs2, make sure FilesApp correctly select between
        # the two:
        doc2 = create_random_file(fs2, db)
        conn = client.connect()
        response = conn.request('GET', '/files/{}'.format(doc2['_id']), {}, None)
        self.assertEqual(response.status, 200)
        self.assertEqual(response.reason, 'OK')
        self.assertEqual(response.headers, {'content-length': doc2['bytes']})
        self.assertIs(response.body.chunked, False)
        h = Hasher()
        index = 0
        while True:
            data = response.body.read(h.protocol.leaf_size)
            if not data:
                break
            leaf = Leaf(index, data)
            h.hash_leaf(leaf)
            index += 1
        ch2 = h.content_hash()
        self.assertEqual(ch2.id, doc2['_id'])
        self.assertEqual(ch2.file_size, doc2['bytes'])

        # Delete file in fs1, make sure FileApps returns 404 when database says
        # file should be in a FileStore but it isn't:
        fs1.remove(ch1.id)
        response = conn.request('GET', '/files/{}'.format(doc1['_id']), {}, None)
        self.assertEqual(response.status, 404)
        self.assertEqual(response.reason, 'Not Found')
        self.assertEqual(response.headers, {})
        self.assertIsNone(response.body)
        self.assertIs(conn.closed, False)

        # "disconnect" fs2, try requesting file2 when no local store has the
        # file, even though the file doc is in the database:
        del machine['stores'][fs2.id]
        db.save(machine)
        response = conn.request('GET', '/files/{}'.format(doc2['_id']), {}, None)
        self.assertEqual(response.status, 404)
        self.assertEqual(response.reason, 'Not Found')
        self.assertEqual(response.headers, {})
        self.assertIsNone(response.body)
        self.assertIs(conn.closed, False)
        del conn


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
        self.assertEqual(app._authorization, 
            microfiber.basic_auth_header(env['basic'])
        )

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
            pki.server_sslconfig, IPv6_LOOPBACK, rgiapps.ProxyApp(env_B)
        )
        env_proxy_B = {
            'url': address_to_url('https', httpd.address),
            'ssl': pki.client_sslconfig,
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
            pki.server_sslconfig, IPv6_LOOPBACK, rgiapps.ProxyApp(env_A)
        )
        env_proxy_A = {
            'url': address_to_url('https', httpd.address),
            'ssl': pki.client_sslconfig,
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
            self.assertEqual(app({}, {'method': method}, default_bodies),
                (405, 'Method Not Allowed', {}, None)
            )

        # path:
        bad_id1 = random_id(30)[:-1] + '0'  # Invalid letter
        request = {'method': 'GET', 'script': [], 'path': [bad_id1]}
        self.assertEqual(app({}, request, default_bodies),
            (400, 'Bad File ID', {}, None)
        )
        self.assertEqual(request,
            {'method': 'GET', 'script': [bad_id1], 'path': []}
        )

        bad_id2 = random_id(25)  # Wrong length
        request = {'method': 'GET', 'script': [], 'path': [bad_id2]}
        self.assertEqual(app({}, request, default_bodies),
            (400, 'Bad File ID Length', {}, None)
        )
        self.assertEqual(request,
            {'method': 'GET', 'script': [bad_id2], 'path': []}
        )

        good_id = random_id(30)
        request = {'method': 'GET', 'script': [], 'path': [good_id, 'more']}
        self.assertEqual(app({}, request, default_bodies),
            (410, 'Gone', {}, None)
        )
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
        self.assertEqual(app({}, request, default_bodies),
            (400, 'No Query For You', {}, None)
        )
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
        self.assertEqual(app({}, request, default_bodies),
            (400, 'Cannot Range with HEAD', {}, None)
        )
        self.assertEqual(request, {
            'method': 'HEAD',
            'script': [good_id],
            'path': [],
            'query': '',
            'headers': {'range': 'bytes=500-1000'},
        })


class TestInfoApp(TestCase):
    def test_init(self):
        _id = random_id(30)
        app = rgiapps.InfoApp(_id)
        self.assertIs(app.id, _id)
        self.assertEqual(
            app.body,
            microfiber.dumps(
                {
                    'id': _id,
                    'version': dmedia.__version__,
                    'user': os.environ.get('USER'),
                    'host': socket.gethostname(),
                }
            ).encode('utf-8')
        )

    def test_call(self):
        app = rgiapps.InfoApp(random_id(30))

        # request['path']
        request = {
            'path': ['foo'],
        }
        self.assertEqual(app({}, request, default_bodies),
            (410, 'Gone', {}, None)
        )

        # request['method']
        for method in ('PUT', 'POST', 'HEAD', 'DELETE'):
            request = {
                'method': method,
                'path': [],
            }
            self.assertEqual(app({}, request, default_bodies),
                (405, 'Method Not Allowed', {}, None)
            )

        # Test when it's all good
        request = {
            'method': 'GET',
            'uri': '/',
            'script': [],
            'path': [],
            'headers': {},
            'body': None,
        }
        self.assertEqual(app({}, request, default_bodies),
            (200, 'OK', {'content-type': 'application/json'}, app.body)
        )

