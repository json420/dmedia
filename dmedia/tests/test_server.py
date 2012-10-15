# dmedia: distributed media library
# Copyright (C) 2011 Novacut Inc
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
Unit tests for `dmedia.server`.
"""

from unittest import TestCase
import multiprocessing
import time
from copy import deepcopy
import os
import socket
from queue import Queue

from usercouch.misc import TempCouch
from filestore import DIGEST_B32LEN, DIGEST_BYTES
import microfiber
from microfiber import random_id, dumps

from .base import TempDir
import dmedia
from dmedia.httpd import make_server, WSGIError
from dmedia.peering import encode, decode
from dmedia import server, client, peering


def random_dbname():
    return 'db-' + microfiber.random_id().lower()


class StartResponse:
    def __init__(self):
        self.__called = False

    def __call__(self, status, headers):
        assert self.__called is False
        self.__called = True
        self.status = status
        self.headers = headers


class TestFunctions(TestCase):
    def test_range_to_slice(self):
        with self.assertRaises(WSGIError) as cm:
            (start, stop) = server.range_to_slice('goats=0-500')
        self.assertEqual(cm.exception.status, '400 Bad Range Units')

        with self.assertRaises(WSGIError) as cm:
            (start, stop) = server.range_to_slice('bytes=-500-999')
        self.assertEqual(cm.exception.status, '400 Bad Range Negative Start')

        with self.assertRaises(WSGIError) as cm:
            (start, stop) = server.range_to_slice('bytes=-foo')
        self.assertEqual(cm.exception.status, '400 Bad Range Negative Start')

        with self.assertRaises(WSGIError) as cm:
            (start, stop) = server.range_to_slice('bytes=500')
        self.assertEqual(cm.exception.status, '400 Bad Range Format')

        with self.assertRaises(WSGIError) as cm:
            (start, stop) = server.range_to_slice('bytes=foo-999')
        self.assertEqual(cm.exception.status, '400 Bad Range Start')

        with self.assertRaises(WSGIError) as cm:
            (start, stop) = server.range_to_slice('bytes=500-bar')
        self.assertEqual(cm.exception.status, '400 Bad Range End')

        with self.assertRaises(WSGIError) as cm:
            (start, stop) = server.range_to_slice('bytes=500-499')
        self.assertEqual(cm.exception.status, '400 Bad Range')

        self.assertEqual(
            server.range_to_slice('bytes=0-0'), (0, 1)
        )
        self.assertEqual(
            server.range_to_slice('bytes=0-499'), (0, 500)
        )
        self.assertEqual(
            server.range_to_slice('bytes=500-999'), (500, 1000)
        )
        self.assertEqual(
            server.range_to_slice('bytes=9500-9999'), (9500, 10000)
        )
        self.assertEqual(
            server.range_to_slice('bytes=9500-'), (9500, None)
        )
        self.assertEqual(
            server.range_to_slice('bytes=-500'), (-500, None)
        )

        # Test the round-trip with client.bytes_range
        slices = [
            (0, 1),
            (0, 500),
            (500, 1000),
            (9500, 10000),
            (-500, None),
            (9500, None),
        ]
        for (start, stop) in slices:
            self.assertEqual(
                server.range_to_slice(client.bytes_range(start, stop)),
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
        }
        app = server.RootApp(env)
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
        self.assertEqual(app.info_length, str(int(len(app.info))))
        self.assertIsInstance(app.proxy, server.ProxyApp)
        self.assertIsInstance(app.files, server.FilesApp)
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
        }
        app = server.RootApp(env)

        # SSL_CLIENT_VERIFY
        with self.assertRaises(WSGIError) as cm:
            app({}, None)
        self.assertEqual(cm.exception.status, '403 Forbidden SSL')
        with self.assertRaises(WSGIError) as cm:
            app({'SSL_CLIENT_VERIFY': 'NOPE'}, None)
        self.assertEqual(cm.exception.status, '403 Forbidden SSL')

        # SSL_CLIENT_I_DN_CN
        environ = {
            'SSL_CLIENT_VERIFY': 'SUCCESS',
        }
        with self.assertRaises(WSGIError) as cm:
            app(environ, None)
        self.assertEqual(cm.exception.status, '403 Forbidden Issuer')
        environ = {
            'SSL_CLIENT_VERIFY': 'SUCCESS',
            'SSL_CLIENT_I_DN_CN': random_id(30),
        }
        with self.assertRaises(WSGIError) as cm:
            app(environ, None)
        self.assertEqual(cm.exception.status, '403 Forbidden Issuer')

        # PATH_INFO
        environ = {
            'SSL_CLIENT_VERIFY': 'SUCCESS',
            'SSL_CLIENT_I_DN_CN': user_id,
            'PATH_INFO': '/foo',
        }
        with self.assertRaises(WSGIError) as cm:
            app(environ, None)
        self.assertEqual(cm.exception.status, '410 Gone')

        # REQUEST_METHOD
        environ = {
            'SSL_CLIENT_VERIFY': 'SUCCESS',
            'SSL_CLIENT_I_DN_CN': user_id,
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'HEAD',
        }
        with self.assertRaises(WSGIError) as cm:
            app(environ, None)
        self.assertEqual(cm.exception.status, '405 Method Not Allowed')

        # Test when it's all good
        environ = {
            'SSL_CLIENT_VERIFY': 'SUCCESS',
            'SSL_CLIENT_I_DN_CN': user_id,
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
        }
        sr = StartResponse()
        ret = app(environ, sr)
        self.assertEqual(ret, [app.info])
        self.assertEqual(sr.status, '200 OK')
        self.assertEqual(sr.headers,
            [
                ('Content-Length', app.info_length),
                ('Content-Type', 'application/json'),
            ]
        )


class TestInfoApp(TestCase):
    def test_init(self):
        _id = random_id(30)
        app = server.InfoApp(_id)
        self.assertIs(app.id, _id)
        self.assertEqual(
            app.info,
            microfiber.dumps(
                {
                    'id': _id,
                    'version': dmedia.__version__,
                    'user': os.environ.get('USER'),
                    'host': socket.gethostname(),
                }   
            ).encode('utf-8')
        )
        self.assertEqual(app.info_length, str(int(len(app.info))))

    def test_call(self):
        app = server.InfoApp(random_id(30))

        # wsgi.multithread
        with self.assertRaises(WSGIError) as cm:
            app({'wsgi.multithread': True}, None)
        self.assertEqual(cm.exception.status, '500 Internal Server Error')
        with self.assertRaises(WSGIError) as cm:
            app({'wsgi.multithread': 0}, None)
        self.assertEqual(cm.exception.status, '500 Internal Server Error')

        # PATH_INFO
        environ = {
            'wsgi.multithread': False,
            'PATH_INFO': '/foo',
        }
        with self.assertRaises(WSGIError) as cm:
            app(environ, None)
        self.assertEqual(cm.exception.status, '410 Gone')

        # REQUEST_METHOD
        environ = {
            'wsgi.multithread': False,
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'HEAD',
        }
        with self.assertRaises(WSGIError) as cm:
            app(environ, None)
        self.assertEqual(cm.exception.status, '405 Method Not Allowed')

        # Test when it's all good
        environ = {
            'wsgi.multithread': False,
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
        }
        sr = StartResponse()
        ret = app(environ, sr)
        self.assertEqual(ret, [app.info])
        self.assertEqual(sr.status, '200 OK')
        self.assertEqual(sr.headers,
            [
                ('Content-Length', app.info_length),
                ('Content-Type', 'application/json'),   
            ]
        )


class TestClientApp(TestCase):
    def test_init(self):
        id1 = random_id(30)
        id2 = random_id(30)
        cr = peering.ChallengeResponse(id1, id2)
        q = Queue()
        app = server.ClientApp(cr, q)
        self.assertIs(app.cr, cr)
        self.assertIs(app.queue, q)
        self.assertEqual(app.map,
            {
                '/challenge': app.get_challenge,
                '/response': app.put_response,
            }
        )

    def test_call(self):
        _id = random_id(30)
        peer_id = random_id(30)
        cr = peering.ChallengeResponse(_id, peer_id)
        app = server.ClientApp(cr, Queue())

        # wsgi.multithread
        with self.assertRaises(WSGIError) as cm:
            app({'wsgi.multithread': True}, None)
        self.assertEqual(cm.exception.status, '500 Internal Server Error')
        with self.assertRaises(WSGIError) as cm:
            app({'wsgi.multithread': 0}, None)
        self.assertEqual(cm.exception.status, '500 Internal Server Error')

        # SSL_CLIENT_VERIFY
        with self.assertRaises(WSGIError) as cm:
            app({'wsgi.multithread': False}, None)
        self.assertEqual(cm.exception.status, '403 Forbidden SSL')
        environ = {
            'wsgi.multithread': False,
            'SSL_CLIENT_VERIFY': 'NOPE',
        }
        with self.assertRaises(WSGIError) as cm:
            app(environ, None)
        self.assertEqual(cm.exception.status, '403 Forbidden SSL')

        # SSL_CLIENT_S_DN_CN
        environ = {
            'wsgi.multithread': False,
            'SSL_CLIENT_VERIFY': 'SUCCESS',
        }
        with self.assertRaises(WSGIError) as cm:
            app(environ, None)
        self.assertEqual(cm.exception.status, '403 Forbidden Subject')
        environ = {
            'wsgi.multithread': False,
            'SSL_CLIENT_VERIFY': 'SUCCESS',
            'SSL_CLIENT_S_DN_CN': random_id(30),
        }
        with self.assertRaises(WSGIError) as cm:
            app(environ, None)
        self.assertEqual(cm.exception.status, '403 Forbidden Subject')

        # SSL_CLIENT_I_DN_CN
        environ = {
            'wsgi.multithread': False,
            'SSL_CLIENT_VERIFY': 'SUCCESS',
            'SSL_CLIENT_S_DN_CN': peer_id,
        }
        with self.assertRaises(WSGIError) as cm:
            app(environ, None)
        self.assertEqual(cm.exception.status, '403 Forbidden Issuer')
        environ = {
            'wsgi.multithread': False,
            'SSL_CLIENT_VERIFY': 'SUCCESS',
            'SSL_CLIENT_S_DN_CN': peer_id,
            'SSL_CLIENT_I_DN_CN': random_id(30),
        }
        with self.assertRaises(WSGIError) as cm:
            app(environ, None)
        self.assertEqual(cm.exception.status, '403 Forbidden Issuer')

        # PATH_INFO
        environ = {
            'wsgi.multithread': False,
            'SSL_CLIENT_VERIFY': 'SUCCESS',
            'SSL_CLIENT_S_DN_CN': peer_id,
            'SSL_CLIENT_I_DN_CN': peer_id,
            'PATH_INFO': '/',
        }
        with self.assertRaises(WSGIError) as cm:
            app(environ, None)
        self.assertEqual(cm.exception.status, '410 Gone')

        # state
        environ = {
            'wsgi.multithread': False,
            'SSL_CLIENT_VERIFY': 'SUCCESS',
            'SSL_CLIENT_S_DN_CN': peer_id,
            'SSL_CLIENT_I_DN_CN': peer_id,
            'PATH_INFO': '/challenge',
        }
        with self.assertRaises(WSGIError) as cm:
            app(environ, None)
        self.assertEqual(cm.exception.status, '400 Bad Request Order')

        # REQUEST_METHOD
        app.state = 'ready'
        environ = {
            'wsgi.multithread': False,
            'SSL_CLIENT_VERIFY': 'SUCCESS',
            'SSL_CLIENT_S_DN_CN': peer_id,
            'SSL_CLIENT_I_DN_CN': peer_id,
            'PATH_INFO': '/challenge',
            'REQUEST_METHOD': 'POST',
        }
        with self.assertRaises(WSGIError) as cm:
            app(environ, None)
        self.assertEqual(cm.exception.status, '405 Method Not Allowed')
        self.assertEqual(app.state, 'gave_challenge')

        # Test when it's all good
        app.state = 'ready'
        environ = {
            'wsgi.multithread': False,
            'SSL_CLIENT_VERIFY': 'SUCCESS',
            'SSL_CLIENT_S_DN_CN': peer_id,
            'SSL_CLIENT_I_DN_CN': peer_id,
            'PATH_INFO': '/challenge',
            'REQUEST_METHOD': 'GET',
        }
        sr = StartResponse()
        ret = app(environ, sr)
        data = dumps({'challenge': encode(cr.challenge)}).encode('utf-8')
        self.assertEqual(ret, [data])
        self.assertEqual(sr.status, '200 OK')
        self.assertEqual(sr.headers,
            [
                ('Content-Length', str(len(data))),
                ('Content-Type', 'application/json'),   
            ]
        )
        self.assertEqual(app.state, 'gave_challenge')



####################################
# Live test cases using Dmedia HTTPD

class TempHTTPD:
    def __init__(self, couch_env, ssl_config):
        """
        :param couch_env: env of the UserCouch we are the SSL frontend for.
        :param ssl_config: dict containing server SSL config.
        """
        queue = multiprocessing.Queue()
        self.process = multiprocessing.Process(
            target=server.run_server,
            args=(queue, couch_env, '::1', ssl_config),
        )
        self.process.daemon = True
        self.process.start()
        self.env = queue.get()
        if isinstance(self.env, Exception):
            raise self.env
        self.port = self.env['port']

    def __del__(self):
        self.process.terminate()
        self.process.join()


class TestRootAppLive(TestCase):
    def test_call(self):
        couch = TempCouch()
        couch_env = couch.bootstrap()
        couch_env['user_id'] = random_id(30)
        couch_env['machine_id'] = random_id(30)

        # Test when client PKI isn't configured
        pki = peering.TempPKI()
        httpd = TempHTTPD(couch_env, pki.get_server_config())
        env = deepcopy(httpd.env)
        env['ssl'] = pki.get_client_config()
        client = microfiber.CouchBase(env)
        with self.assertRaises(microfiber.Forbidden) as cm:
            client.get()
        self.assertEqual(cm.exception.response.reason, 'Forbidden SSL')

        # Test when cert issuer is wrong
        pki = peering.TempPKI(True)
        httpd = TempHTTPD(couch_env, pki.get_server_config())
        env = deepcopy(httpd.env)
        env['ssl'] = pki.get_client_config()
        client = microfiber.CouchBase(env)
        with self.assertRaises(microfiber.Forbidden) as cm:
            client.get()
        self.assertEqual(cm.exception.response.reason, 'Forbidden Issuer')

        # Test when SSL config is correct, then test other aspects
        pki = peering.TempPKI(True)
        couch_env['user_id'] = pki.client_ca.id
        couch_env['machine_id'] = pki.server.id
        httpd = TempHTTPD(couch_env, pki.get_server_config())
        env = deepcopy(httpd.env)
        env['ssl'] = pki.get_client_config()
        client = microfiber.CouchBase(env)
        self.assertEqual(client.get(),
            {
                'user_id': pki.client_ca.id,
                'machine_id': pki.server.id,
                'version': dmedia.__version__,
                'user': os.environ.get('USER'),
                'host': socket.gethostname(),
            }
        )
        self.assertEqual(client.get('couch')['couchdb'], 'Welcome')

        with self.assertRaises(microfiber.MethodNotAllowed) as cm:
            client.put(None)

        with self.assertRaises(microfiber.Gone) as cm:
            client.get('foo')

        with self.assertRaises(microfiber.Forbidden) as cm:
            client.get('couch', '_config')

    def test_replication(self):
        """
        Test push replication Couch1 => HTTPD => Couch2.
        """
        pki = peering.TempPKI(True)
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
        httpd = TempHTTPD(env2, pki.get_server_config())

        # Create just the source DB, rely on create_target=True for remote
        name1 = random_dbname()
        name2 = random_dbname()
        self.assertEqual(s1.put(None, name1), {'ok': True})

        env = {'url': 'https://[::1]:{}/couch/'.format(httpd.port)}
        result = s1.push(name1, name2, env, continuous=True, create_target=True)
        self.assertEqual(set(result), set(['_local_id', 'ok']))
        self.assertIs(result['ok'], True)

        # Save docs in s1.name1, make sure they show up in s2.name2
        docs = [{'_id': random_id()} for i in range(100)]
        for doc in docs:
            doc['_rev'] = s1.post(doc, name1)['rev']
        time.sleep(0.5)
        for doc in docs:
            self.assertEqual(s2.get(name2, doc['_id']), doc)


class TestServerAppLive(TestCase):
    def test_live(self):
        tmp = TempDir()
        pki = peering.PKI(tmp.dir)
        local_id = pki.create_key()
        pki.create_ca(local_id)
        remote_id = pki.create_key()
        pki.create_ca(remote_id)
        server_config = {
            'cert_file': pki.path(local_id, 'ca'),
            'key_file': pki.path(local_id, 'key'),
            'ca_file': pki.path(remote_id, 'ca'),
        }
        client_config = {
            'check_hostname': False,
            'ca_file': pki.path(local_id, 'ca'),
            'cert_file': pki.path(remote_id, 'ca'),
            'key_file': pki.path(remote_id, 'key'),
        }
        local = peering.ChallengeResponse(local_id, remote_id)
        remote = peering.ChallengeResponse(remote_id, local_id)
        q = Queue()
        app = server.ServerApp(local, q, None)
        httpd = make_server(app, '127.0.0.1', server_config)
        client = microfiber.CouchBase(
            {'url': httpd.url, 'ssl': client_config}
        )
        httpd.start()
        secret = local.get_secret()
        remote.set_secret(secret)

        self.assertIsNone(app.state)
        with self.assertRaises(microfiber.BadRequest) as cm:
            client.get('')
        self.assertEqual(
            str(cm.exception),
            '400 Bad Request State: GET /'
        )
        app.state = 'info'
        self.assertEqual(client.get(),
            {
                'id': local_id,
                'user': os.environ.get('USER'),
                'host': socket.gethostname(),
            }
        )
        self.assertEqual(app.state, 'ready')
        with self.assertRaises(microfiber.BadRequest) as cm:
            client.get('')
        self.assertEqual(
            str(cm.exception),
            '400 Bad Request State: GET /'
        )
        self.assertEqual(app.state, 'ready')

        app.state = 'info'
        with self.assertRaises(microfiber.BadRequest) as cm:
            client.get('challenge')
        self.assertEqual(
            str(cm.exception),
            '400 Bad Request Order: GET /challenge'
        )
        with self.assertRaises(microfiber.BadRequest) as cm:
            client.put({'hello': 'world'}, 'response')
        self.assertEqual(
            str(cm.exception),
            '400 Bad Request Order: PUT /response'
        )

        app.state = 'ready'
        self.assertEqual(app.state, 'ready')
        obj = client.get('challenge')
        self.assertEqual(app.state, 'gave_challenge')
        self.assertIsInstance(obj, dict)
        self.assertEqual(set(obj), set(['challenge']))
        self.assertEqual(local.challenge, decode(obj['challenge']))
        with self.assertRaises(microfiber.BadRequest) as cm:
            client.get('challenge')
        self.assertEqual(
            str(cm.exception),
            '400 Bad Request Order: GET /challenge'
        )
        self.assertEqual(app.state, 'gave_challenge')

        (nonce, response) = remote.create_response(obj['challenge'])
        obj = {'nonce': nonce, 'response': response}
        self.assertEqual(client.put(obj, 'response'), {'ok': True})
        self.assertEqual(app.state, 'response_ok')
        with self.assertRaises(microfiber.BadRequest) as cm:
            client.put(obj, 'response')
        self.assertEqual(
            str(cm.exception),
            '400 Bad Request Order: PUT /response'
        )
        self.assertEqual(app.state, 'response_ok')
        self.assertEqual(q.get(), 'response_ok')

        # Test when an error occurs in put_response()
        app.state = 'gave_challenge'
        with self.assertRaises(microfiber.ServerError) as cm:
            client.put(b'bad json', 'response')
        self.assertEqual(app.state, 'in_response')

        # Test with wrong secret
        app.state = 'ready'
        secret = local.get_secret()
        remote.get_secret()
        challenge = client.get('challenge')['challenge']
        self.assertEqual(app.state, 'gave_challenge')
        (nonce, response) = remote.create_response(challenge)
        with self.assertRaises(microfiber.Unauthorized) as cm:
            client.put({'nonce': nonce, 'response': response}, 'response')
        self.assertEqual(app.state, 'wrong_response')
        self.assertFalse(hasattr(local, 'secret'))
        self.assertFalse(hasattr(local, 'challenge'))

        # Verify that you can't retry
        remote.set_secret(secret)
        (nonce, response) = remote.create_response(challenge)
        with self.assertRaises(microfiber.BadRequest) as cm:
            client.put({'nonce': nonce, 'response': response}, 'response')
        self.assertEqual(
            str(cm.exception),
            '400 Bad Request Order: PUT /response'
        )
        self.assertEqual(app.state, 'wrong_response')
        self.assertEqual(q.get(), 'wrong_response')

        httpd.shutdown()
