#!/usr/bin/python3

import logging
import tempfile
import socket
import os
import multiprocessing

from gi.repository import GObject
from microfiber import dumps

from dmedia.startup import DmediaCouch
from dmedia.service.peers import Peer
from dmedia.httpd import run_server, WSGIError


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


def start_server_process(ssl_config):
    queue = multiprocessing.Queue()
    process = multiprocessing.Process(
        target=run_server,
        args=(queue, server_info, '0.0.0.0', ssl_config),
    )
    process.daemon = True
    process.start()
    env = queue.get()
    if isinstance(env, Exception):
        raise env
    return (process, env['port'])


mainloop = GObject.MainLoop()
couch = DmediaCouch(tempfile.mkdtemp())
couch.firstrun_init(create_user=False)
couch.load_pki()
machine_id = couch.pki.machine.id
avahi = Peer(machine_id, couch.pki)
ssl_config = {
    'key_file': couch.pki.machine.key_file,
    'cert_file': couch.pki.machine.cert_file,
}

(httpd, port) = start_server_process(ssl_config)
avahi.browse('_dmedia-accept._tcp')
avahi.publish('_dmedia-offer._tcp', port)
mainloop.run()


