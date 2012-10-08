#!/usr/bin/python3

import logging
import tempfile
import socket
import os
from queue import Queue
from gettext import gettext as _

from gi.repository import GObject, Gtk
from microfiber import dumps, CouchBase, Unauthorized, _start_thread

from dmedia.startup import DmediaCouch
from dmedia import peering
from dmedia.service.peers import AvahiPeer
from dmedia.gtk.peering import BaseUI
from dmedia.peering import ChallengeResponse, ChallengeResponseApp
from dmedia.httpd import WSGIError, make_server, build_server_ssl_context


format = [
    '%(levelname)s',
    '%(processName)s',
    '%(threadName)s',
    '%(message)s',
]
logging.basicConfig(level=logging.DEBUG, format='\t'.join(format))
log = logging.getLogger()


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


class Session:
    def __init__(self, hub, _id, peer, client_config):
        self.hub = hub
        self.peer = peer
        self.peer_id = peer.id
        self.cr = ChallengeResponse(_id, peer.id)
        self.q = Queue()
        _start_thread(self.monitor_q)
        self.app = ChallengeResponseApp(self.cr, self.q)
        self.app.state = 'ready'
        env = {'url': peer.url, 'ssl': client_config}
        self.client = CouchBase(env)

    def challenge(self):
        log.info('Getting challenge from %r', self.peer)
        challenge = self.client.get('challenge')['challenge']
        (nonce, response) = self.cr.create_response(challenge)
        obj = {'nonce': nonce, 'response': response}
        log.info('Putting response to %r', self.peer)
        try:
            r = self.client.put(obj, 'response')
            log.info('Response accepted')
            success = True
        except Unauthorized:
            log.info('Response rejected')
            success = False
        GObject.idle_add(self.hub.send, 'challenge', success)

    def monitor_q(self):
        status = self.q.get()
        log.info('Counter-response gave %r', status)
        success = (status == 'response_ok')
        if not success:
            log.error('Wrong counter-response!')
            log.warning('Possible malicious peer: %r', self.peer)
        GObject.idle_add(self.hub.send, 'counter_challenge', success)


class UI(BaseUI):
    page = 'client.html'

    signals = {
        'first_time': [],
        'already_using': [],
        'have_secret': ['secret'],
        'challenge': ['success'],
        'counter_challenge': ['success'],
        'set_message': ['message'],
        'show_screen2b': [],
        'show_screen3b': [],
    }

    def __init__(self):
        super().__init__()
        self.window.connect('destroy', Gtk.main_quit)
        self.couch = DmediaCouch(tempfile.mkdtemp())
        self.couch.firstrun_init(create_user=False)
        self.couch.load_pki()
        self.avahi = None

    def connect_hub_signals(self, hub):
        hub.connect('first_time', self.on_first_time)
        hub.connect('already_using', self.on_already_using)
        hub.connect('have_secret', self.on_have_secret)
        hub.connect('challenge', self.on_challenge)
        hub.connect('counter_challenge', self.on_counter_challenge)

    def on_first_time(self, hub):
        print('on_first_time')

    def on_already_using(self, hub):
        if self.avahi is not None:
            print('oop, duplicate click')
            return
        self.avahi = AvahiPeer(self.couch.pki, client_mode=True)
        self.avahi.connect('accept', self.on_accept)
        self.httpd = make_server(server_info, '0.0.0.0',
            self.avahi.get_server_config()
        )
        self.httpd.start()
        self.avahi.browse()
        self.avahi.publish(self.httpd.port)
        GObject.idle_add(hub.send, 'show_screen2b')

    def on_accept(self, avahi, peer):
        self.avahi.activate(peer.id)
        self.session = Session(self.hub, avahi.id, peer,
            avahi.get_client_config()
        )
        # Reconfigure HTTPD to only accept connections from bound peer
        self.httpd.reconfigure(self.session.app, avahi.get_server_config())
        avahi.unpublish()
        GObject.idle_add(self.hub.send, 'show_screen3b')

    def on_have_secret(self, hub, secret):
        hub.send('set_message', _('Challenge...'))
        self.session.cr.set_secret(secret)
        _start_thread(self.session.challenge)

    def on_challenge(self, hub, success):
        if success:
            hub.send('set_message', _('Counter-Challenge...'))
        else:
            hub.send('set_message', _('Typo? Please try again with new secret.'))

    def on_counter_challenge(self, hub, success):
        if success:
            hub.send('set_message', _('Requesting Certificate...'))
        else:
            hub.send('set_message', _('Very Bad Things!'))


ui = UI()
ui.run()
