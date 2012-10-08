#!/usr/bin/python3

import logging
import tempfile
import socket
import os

from gi.repository import GObject
from microfiber import dumps

from dmedia.startup import DmediaCouch
from dmedia.service.peers import AvahiPeer
from dmedia.httpd import WSGIError, make_server


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
couch.firstrun_init(create_user=True)
couch.load_pki()

def on_offer(avahi, info):
    avahi.httpd = make_server(server_info, '0.0.0.0',
        avahi.get_server_config()
    )
    avahi.httpd.start()
    avahi.accept(info.id, avahi.httpd.port)

avahi = AvahiPeer(couch.pki)
avahi.connect('offer', on_offer)
avahi.browse()



mainloop.run()


