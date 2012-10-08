#!/usr/bin/python3

import logging
import tempfile
import socket
import os

from gi.repository import GObject
from microfiber import dumps

from dmedia.startup import DmediaCouch
from dmedia.service.peers import AvahiPeer
from dmedia.httpd import WSGIError, make_server, build_server_ssl_context


format = [
    '%(levelname)s',
    '%(processName)s',
    '%(threadName)s',
    '%(message)s',
]
logging.basicConfig(level=logging.DEBUG, format='\t'.join(format))


INFO = dumps(
    {'user': os.environ['USER'], 'host': socket.gethostname()}
).encode('utf-8')
INFO_LENGTH = str(len(INFO))


def server_info(environ, start_response):
    if environ['REQUEST_METHOD'] != 'GET':
        raise WSGIError('405 Method Not Allowed')
    start_response('200 OK',
        [
            ('Content-Length', INFO_LENGTH),
            ('Content-Type', 'application/json'),
        ]
    )
    return [INFO]


mainloop = GObject.MainLoop()
couch = DmediaCouch(tempfile.mkdtemp())
couch.firstrun_init(create_user=False)
couch.load_pki()

def on_accept(avahi, info):
    print(info)
    avahi.activate(info.id)
    # Reconfigure HTTPD to only accept connections from bound peer
    httpd = avahi.httpd
    httpd.shutdown()
    httpd.context = build_server_ssl_context(avahi.get_server_config())
    httpd.start()
    avahi.unpublish()

avahi = AvahiPeer(couch.pki, client_mode=True)
avahi.connect('accept', on_accept)
httpd = make_server(server_info, '0.0.0.0', avahi.get_server_config())
httpd.start()
avahi.httpd = httpd
avahi.browse()
avahi.publish(httpd.port)
mainloop.run()


