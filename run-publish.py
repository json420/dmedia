#!/usr/bin/python3

import logging
import tempfile
from queue import Queue
from gettext import gettext as _
from base64 import b64encode, b64decode

from gi.repository import GObject, Gtk
from microfiber import dumps, CouchBase, Unauthorized, _start_thread

from dmedia.startup import DmediaCouch
from dmedia import peering
from dmedia.service.peers import AvahiPeer
from dmedia.gtk.peering import BaseUI
from dmedia.peering import ChallengeResponse, ClientApp, InfoApp, encode, decode
from dmedia.httpd import WSGIError, make_server, build_server_ssl_context


format = [
    '%(levelname)s',
    '%(processName)s',
    '%(threadName)s',
    '%(message)s',
]
logging.basicConfig(level=logging.DEBUG, format='\t'.join(format))
log = logging.getLogger()


class Session:
    def __init__(self, hub, pki, _id, peer, client_config):
        self.hub = hub
        self.pki = pki
        self.peer = peer
        self.id = _id
        self.peer_id = peer.id
        self.cr = ChallengeResponse(_id, peer.id)
        self.q = Queue()
        self.app = ClientApp(self.cr, self.q)
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
        GObject.idle_add(self.on_response, success)

    def on_response(self, success):
        if success:
            self.app.state = 'ready'
            _start_thread(self.monitor_counter_response)
        else:
            del self.cr.secret
        self.hub.send('response', success)

    def monitor_counter_response(self):
        # FIXME: Should use a timeout with queue.get()
        status = self.q.get()
        log.info('Counter-response gave %r', status)
        if status != 'response_ok':
            log.error('Wrong counter-response!')
            log.warning('Possible malicious peer: %r', self.peer)
        GObject.timeout_add(500, self.on_counter_response, status)

    def on_counter_response(self, status):
        assert self.app.state == status
        if status == 'response_ok':
            _start_thread(self.request_cert)
        self.hub.send('counter_response', status)

    def request_cert(self):
        log.info('Creating CSR')
        try:
            self.pki.create_csr(self.id)
            csr_data = self.pki.read_csr(self.id)
            obj = {'csr': b64encode(csr_data).decode('utf-8')}
            r = self.client.post(obj, 'csr')
            cert_data = b64decode(r['cert'].encode('utf-8'))
            self.pki.write_cert2(self.id, self.peer_id, cert_data)
            self.pki.verify_cert2(self.id, self.peer_id)
            status = 'cert_issued'
        except Exception as e:
            status = 'error'
            log.exception('Could not request cert')
        GObject.idle_add(self.on_csr_response, status)

    def on_csr_response(self, status):
        log.info('on_csr_response %r', status)
        self.hub.send('csr_response', status)


class UI(BaseUI):
    page = 'client.html'

    signals = {
        'first_time': [],
        'already_using': [],
        'have_secret': ['secret'],
        'response': ['success'],
        'counter_response': ['status'],
        'csr_response': ['status'],
        'set_message': ['message'],

        'show_screen2a': [],
        'show_screen2b': [],
        'show_screen3b': [],
    }

    def __init__(self):
        super().__init__()
        self.couch = DmediaCouch(tempfile.mkdtemp())
        self.couch.firstrun_init(create_user=False)
        self.couch.load_pki()
        self.avahi = None

    def quit(self, *args):
        if self.avahi:
            self.avahi.unpublish()
        Gtk.main_quit()

    def connect_hub_signals(self, hub):
        hub.connect('first_time', self.on_first_time)
        hub.connect('already_using', self.on_already_using)
        hub.connect('have_secret', self.on_have_secret)
        hub.connect('response', self.on_response)
        hub.connect('counter_response', self.on_counter_response)
        hub.connect('csr_response', self.on_csr_response)

    def on_first_time(self, hub):
        hub.send('show_screen2a')

    def on_already_using(self, hub):
        if self.avahi is not None:
            print('oop, duplicate click')
            return
        self.avahi = AvahiPeer(self.couch.pki, client_mode=True)
        self.avahi.connect('accept', self.on_accept)
        app = InfoApp(self.avahi.id)
        self.httpd = make_server(app, '0.0.0.0',
            self.avahi.get_server_config()
        )
        self.httpd.start()
        self.avahi.browse()
        self.avahi.publish(self.httpd.port)
        GObject.idle_add(hub.send, 'show_screen2b')

    def on_accept(self, avahi, peer):
        self.avahi.activate(peer.id)
        self.session = Session(self.hub, self.couch.pki, avahi.id, peer,
            avahi.get_client_config()
        )
        # Reconfigure HTTPD to only accept connections from bound peer
        self.httpd.reconfigure(self.session.app, avahi.get_server_config())
        avahi.unpublish()
        GObject.idle_add(self.hub.send, 'show_screen3b')

    def on_have_secret(self, hub, secret):
        if hasattr(self.session.cr, 'secret'):
            log.warning("duplicate 'have_secret' signal received")
            return
        self.session.cr.set_secret(secret)
        hub.send('set_message', _('Challenge...'))
        _start_thread(self.session.challenge)

    def on_response(self, hub, success):
        if success:
            hub.send('set_message', _('Counter-Challenge...'))
        else:
            hub.send('set_message', _('Typo? Please try again with new secret.'))

    def on_counter_response(self, hub, status):
        if status == 'response_ok':
            hub.send('set_message', _('Requesting Certificate...'))
        else:
            hub.send('set_message', _('Very Bad Things!'))

    def on_csr_response(self, hub, status):
        if status == 'cert_issued':
            hub.send('set_message', _('Done!'))
        else:
            hub.send('set_message', _('Very Bad Things with Certificate!'))


ui = UI()
ui.run()
