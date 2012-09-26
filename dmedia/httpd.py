# dmedia: distributed media library
# Copyright (C) 2012 Novacut Inc
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
A tiny WSGI HTTP 1.1 server with IPv6 and SSL support.
"""

import socket
import ssl
import select
import threading

from usercouch import bind_socket, build_url
from dmedia import __version__


TYPE_ERROR = '{}: need a {!r}; got a {!r}: {!r}'


def start_thread(target, *args):
    thread = threading.Thread(target=target, args=args)
    thread.daemon = True
    thread.start()
    return thread
    
    
def build_ssl_server_context(config):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
    ctx.load_cert_chain(config['cert_file'],
        keyfile=config.get('key_file')
    )
    if 'ca_file' in config or 'ca_path' in config:
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_verify_locations(
            cafile=config.get('ca_file'),
            capath=config.get('ca_path'),
        )
    return ctx


class Handler:
    def __init__(self, app, conn, address):
        self.app = app
        self.conn = conn
        self.address = address


class Server:
    protocol = 'HTTP/1.1'
    software = 'Dmedia/' + __version__

    def __init__(self, app, bind_address='::1', context=None, threaded=False):
        if not callable(app):
            raise TypeError('app not callable: {!r}'.format(app))
        if not (context is None or isinstance(context, ssl.SSLContext)):
            raise TypeError(TYPE_ERROR.format(
                'context', ssl.SSLContext, type(context), context)
            )
        self.app = app
        self.socket = bind_socket(bind_address)
        (host, port) = self.socket.getsockname()[:2]
        self.name = socket.getfqdn(host)
        self.port = port
        self.context = context
        self.threaded = threaded
        self.scheme = ('http' if context is None else 'https')
        self.url = build_url(self.scheme, bind_address, port)
        self.environ = self.build_base_environ()

    def build_base_environ(self):
        return {
            'wsgi.version': '(1, 0)',
            'wsgi.url_scheme': self.scheme,
            'wsgi.multithread': self.threaded,
            'wsgi.multiprocess': False,
            'wsgi.run_once': False,

            'SERVER_SOFTWARE': self.software,
            'SERVER_PROTOCOL': self.protocol,
            'SCRIPT_NAME': self.name,
            'SERVER_PORT': str(self.port),
        }

    def serve_forever(self):
        self.socket.listen(5)
        while True:
            (conn, address) = self.socket.accept()
            if self.threaded:
                start_thread(self.handle_connection, conn, address)
            else:
                self.handle_connection(conn, address)

    def handle_connection(self, conn, address):
        if self.context is not None:
            conn = self.context.wrap_socket(conn, server_side=True)
            while True:
                try:
                    conn.do_handshake()
                    break
                except ssl.SSLError as err:
                    if err.args[0] == ssl.SSL_ERROR_WANT_READ:
                        select.select([conn], [], [])
                    elif err.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                        select.select([], [conn], [])
                    else:
                        raise err
        
            
        
    
