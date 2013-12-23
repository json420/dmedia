# dmedia: distributed media library
# Copyright (C) 2012 Novacut Inc
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under the
# terms of the GNU Affero General Public License as published by the Free
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
Browse for Dmedia peer offerings, publish the same.

Existing machines constantly listen for _dmedia-offer._tcp.

New machine publishes _dmedia-offer._tcp, and listens for _dmedia-accept._tcp.

Existing machine prompts user, and if they accept, machine publishes
_dmedia-accept._tcp, which initiates peering process.
"""

import logging
from os import path
from collections import namedtuple
import ssl
import socket
import threading
from queue import Queue
from base64 import b64encode, b64decode
from gettext import gettext as _
from subprocess import Popen
import weakref
import time

import dbus
from gi.repository import GLib, GObject, Gtk, AppIndicator3
from microfiber import Unauthorized, CouchBase
from microfiber import random_id, dumps, build_ssl_context

import dmedia
from dmedia.parallel import start_thread
from dmedia.gtk.ubuntu import NotifyManager
from dmedia.identity import ChallengeResponse
from dmedia.server import ServerApp, InfoApp, ClientApp
from dmedia.httpd import WSGIError, make_server


PROTO = 0  # Protocol -1 = both, 0 = IPv4, 1 = IPv6
Peer = namedtuple('Peer', 'id ip port')
Info = namedtuple('Info', 'name host url id')
log = logging.getLogger()

dmedia_peer_gtk = 'dmedia-peer-gtk'
if path.isfile(path.join(dmedia.TREE, dmedia_peer_gtk)):
    dmedia_peer_gtk = path.join(dmedia.TREE, dmedia_peer_gtk)


def make_url(ip, port):
    if PROTO == 0:
        return 'https://{}:{}/'.format(ip, port)
    elif PROTO == 1:
        return 'https://[{}]:{}/'.format(ip, port)
    raise Exception('bad PROTO')


def get_service(verb):
    """
    Get Avahi service name for appropriate direction.

    For example, for an offer:

    >>> get_service('offer')
    '_dmedia-offer._tcp'

    And for an accept:

    >>> get_service('accept')
    '_dmedia-accept._tcp'

    """
    assert verb in ('offer', 'accept')
    return '_dmedia-{}._tcp'.format(verb)


class State:
    """
    A state machine to help prevent silly mistakes.

    So that threading issues don't make the code difficult to reason about,
    a thread-lock is acquired when making a state change.  To be on the safe
    side, you should only make state changes from the main thread.  But the
    thread-lock is there as a safety in case an attacker could change the
    execution such that something isn't called from the main thread, or in case
    an oversight is made by the programmer.
    """
    def __init__(self):
        self.__state = 'free'
        self.__peer_id = None
        self.__lock = threading.Lock()

    def __repr__(self):
        return 'State(state={!r}, peer_id={!r})'.format(
            self.__state, self.__peer_id
        )

    @property
    def state(self):
        return self.__state

    @property
    def peer_id(self):
        return self.__peer_id

    def bind(self, peer_id):
        with self.__lock:
            assert peer_id is not None
            if self.__state != 'free':
                return False
            if self.__peer_id is not None:
                return False
            self.__state = 'bound'
            self.__peer_id = peer_id
            return True

    def verify(self, peer_id):
        with self.__lock:
            if self.__state != 'bound':
                return False
            if peer_id is None or peer_id != self.__peer_id:
                return False
            self.__state = 'verified'
            return True

    def unbind(self, peer_id):
        with self.__lock:
            if self.__state not in ('bound', 'verified'): 
                return False
            if peer_id is None or peer_id != self.__peer_id:
                return False
            self.__state = 'unbound'
            return True

    def activate(self, peer_id):
        with self.__lock:
            if self.__state != 'verified':
                return False
            if peer_id is None or peer_id != self.__peer_id:
                return False
            self.__state = 'activated'
            self.__peer_id = peer_id
            return True

    def deactivate(self, peer_id):
        with self.__lock:
            if self.__state != 'activated':
                return False
            if peer_id is None or peer_id != self.__peer_id:
                return False
            self.__state = 'deactivated'
            return True

    def free(self, peer_id):
        with self.__lock:
            if self.__state not in ('unbound', 'deactivated'):
                return False
            if peer_id is None or peer_id != self.__peer_id:
                return False
            self.__state = 'free'
            self.__peer_id = None
            return True


class AvahiPeer(GObject.GObject):
    __gsignals__ = {
        'offer': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [GObject.TYPE_PYOBJECT]
        ),
        'accept': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [GObject.TYPE_PYOBJECT]
        ),
        'retract': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            []
        ),
    }

    def __init__(self, pki, client_mode=False):
        super().__init__()
        self.group = None
        self.pki = pki
        self.client_mode = client_mode
        ca = (pki.machine if client_mode else pki.user)
        self.id = ca.id
        self.cert_file = ca.ca_file
        self.key_file = ca.key_file
        self.state = State()
        self.peer = None
        self.info = None
        self.bus = dbus.SystemBus()
        self.avahi = self.bus.get_object('org.freedesktop.Avahi', '/')

    def __del__(self):
        self.unpublish()

    def activate(self, peer_id):
        if not self.state.activate(peer_id):
            raise Exception(
                'Cannot activate {!r} from {!r}'.format(peer_id, self.state)
            )
        log.info('Peering: activated session with %r', self.peer)
        assert self.state.state == 'activated'
        assert self.state.peer_id == peer_id
        assert self.peer.id == peer_id
        assert self.info.id == peer_id
        assert self.info.url == make_url(self.peer.ip, self.peer.port)

    def deactivate(self, peer_id):
        if not self.state.deactivate(peer_id):
            raise Exception(
                'Cannot deactivate {!r} from {!r}'.format(peer_id, self.state)
            )
        log.info('Peering: deactivated session with %r', self.peer)
        assert self.state.state == 'deactivated'
        assert self.state.peer_id == peer_id
        assert self.peer.id == peer_id
        assert self.info.id == peer_id
        assert self.info.url == make_url(self.peer.ip, self.peer.port)
        GLib.timeout_add(15 * 1000, self.on_timeout, peer_id)

    def abort(self, peer_id):
        GLib.idle_add(self.unbind, peer_id)

    def unbind(self, peer_id):
        retract = (self.state.state == 'verified')
        if not self.state.unbind(peer_id):
            log.error('Peering: cannot unbind %s from %r', peer_id, self.state)
            return
        log.info('Peering: unbound from %s', peer_id)
        assert self.state.peer_id == peer_id
        assert self.state.state == 'unbound'
        if retract:
            log.info("Peering: firing 'retract' signal")
            self.emit('retract')
        GLib.timeout_add(10 * 1000, self.on_timeout, peer_id)

    def on_timeout(self, peer_id):
        if not self.state.free(peer_id):
            log.error('Peering: cannot free %s from %r', peer_id, self.state)
            return
        log.info('Peering: rate-limiting timeout reached, freeing from %s', peer_id)
        assert self.state.state == 'free'
        assert self.state.peer_id is None
        self.info = None
        self.peer = None

    def get_server_config(self):
        """
        Get the initial server SSL config.
        """
        assert self.state.state in ('free', 'activated')
        config = {
            'key_file': self.key_file,
            'cert_file': self.cert_file,
        }
        if self.client_mode is False or self.state.state == 'activated':
            config['ca_file'] = self.pki.verify_ca(self.state.peer_id)
        return config

    def get_client_config(self):
        """
        Get the client SSL config.
        """
        assert self.state.state == 'activated'
        return {
            'ca_file': self.pki.verify_ca(self.state.peer_id),
            'check_hostname': False,
            'key_file': self.key_file,
            'cert_file': self.cert_file,
        }

    def publish(self, port):
        verb = ('offer' if self.client_mode else 'accept')
        service = get_service(verb)
        self.group = self.bus.get_object(
            'org.freedesktop.Avahi',
            self.avahi.EntryGroupNew(
                dbus_interface='org.freedesktop.Avahi.Server'
            )
        )
        log.info(
            'Peering: publishing %s for %r on port %s', self.id, service, port
        )
        self.group.AddService(
            -1,  # Interface
            PROTO,  # Protocol -1 = both, 0 = ipv4, 1 = ipv6
            0,  # Flags
            self.id,
            service,
            '',  # Domain, default to .local
            '',  # Host, default to localhost
            port,  # Port
            b'',  # TXT record
            dbus_interface='org.freedesktop.Avahi.EntryGroup'
        )
        self.group.Commit(dbus_interface='org.freedesktop.Avahi.EntryGroup')

    def unpublish(self):
        if self.group is not None:
            log.info('Peering: unpublishing %s', self.id)
            self.group.Reset(dbus_interface='org.freedesktop.Avahi.EntryGroup')
            self.group = None

    def browse(self):
        verb = ('accept' if self.client_mode else 'offer')
        service = get_service(verb)
        log.info('Peering: browsing for %r', service)
        path = self.avahi.ServiceBrowserNew(
            -1,  # Interface
            PROTO,  # Protocol -1 = both, 0 = ipv4, 1 = ipv6
            service,
            '', # Domain, default to .local
            0,  # Flags
            dbus_interface='org.freedesktop.Avahi.Server'
        )
        self.browser = self.bus.get_object('org.freedesktop.Avahi', path)
        self.browser.connect_to_signal('ItemNew', self.on_ItemNew)
        self.browser.connect_to_signal('ItemRemove', self.on_ItemRemove)

    def on_ItemNew(self, interface, protocol, peer_id, _type, domain, flags):
        log.info('Peering: peer added: %s', peer_id)
        if not self.state.bind(str(peer_id)):
            log.error('Peering: cannot bind %s from %r', peer_id, self.state)
            log.warning('Peering: possible attack from %s', peer_id)
            return
        assert self.state.state == 'bound'
        assert self.state.peer_id == peer_id
        log.info('Bound to %s', peer_id)
        self.avahi.ResolveService(
            # 2nd to last arg is Protocol, again for some reason
            interface, protocol, peer_id, _type, domain, PROTO, 0,
            dbus_interface='org.freedesktop.Avahi.Server',
            reply_handler=self.on_reply,
            error_handler=self.on_error,
        )

    def on_reply(self, *args):
        peer_id = args[2]
        if self.state.peer_id != peer_id or self.state.state != 'bound':
            log.error(
                '%s: state mismatch in on_reply(): %r', peer_id, self.state
            )
            return
        (ip, port) = args[7:9]
        log.info('%s is at %s, port %s', peer_id, ip, port)
        self.peer = Peer(str(peer_id), str(ip), int(port))
        start_thread(self.cert_thread, self.peer)

    def on_error(self, error):
        log.error(
            '%s: error calling ResolveService(): %r', self.state.peer_id, error
        )
        self.abort(self.state.peer_id)

    def on_ItemRemove(self, interface, protocol, peer_id, _type, domain, flags):
        log.info('Peer removed: %s', peer_id)
        self.abort(peer_id)

    def cert_thread(self, peer):
        # 1 Retrieve the peer certificate:
        try:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
            ctx.options |= ssl.OP_NO_COMPRESSION
            if self.client_mode:
                # The server will only let its cert be retrieved by the client
                # bound to the peering session
                ctx.load_cert_chain(self.cert_file, self.key_file)
            sock = ctx.wrap_socket(
                socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            )
            sock.connect((peer.ip, peer.port))
            pem = ssl.DER_cert_to_PEM_cert(sock.getpeercert(True))
        except Exception as e:
            log.exception('Could not retrieve cert for %r', peer)
            return self.abort(peer.id)
        log.info('Retrieved cert for %r', peer)

        # 2 Make sure peer cert has correct intrinsic CN, etc:
        try:
            ca_file = self.pki.write_ca(peer.id, pem.encode('ascii'))
        except Exception as e:
            log.exception('Could not verify cert for %r', peer)
            return self.abort(peer.id)
        log.info('Verified cert for %r', peer)

        # 3 Make get request to verify peer has private key:
        try:
            url = make_url(peer.ip, peer.port)
            ssl_config = {
                'ca_file': ca_file,
                'check_hostname': False,
            }
            if self.client_mode:
                ssl_config.update({
                    'key_file': self.key_file,
                    'cert_file': self.cert_file,
                })
            client = CouchBase({'url': url, 'ssl': ssl_config})
            d = client.get()
            info = Info(d['user'], d['host'], url, peer.id)
        except Exception as e:
            log.exception('GET / failed for %r', peer)
            return self.abort(peer.id)
        log.info('GET / succeeded with %r', info)
        GLib.idle_add(self.on_cert_complete, peer, info)

    def on_cert_complete(self, peer, info):
        if not self.state.verify(peer.id):
            log.error(
                '%s: mismatch in on_cert_complete(): %r', peer.id, self.state
            )
            return
        assert self.state.state == 'verified'
        assert self.state.peer_id == peer.id
        assert peer is self.peer
        assert self.info is None
        self.info = info
        log.info('Cert checked-out for %r', peer)
        signal = ('accept' if self.client_mode else 'offer')
        log.info('Firing %r signal for %r', signal, info)
        self.emit(signal, info)


class Session:
    def __init__(self, browser, peer, server_config, client_config):
        self.browser = weakref.proxy(browser)
        self.peer = peer
        self.cr = ChallengeResponse(browser.avahi.id, peer.id)
        self.q = Queue()
        start_thread(self.monitor_response)
        self.app = ServerApp(self.cr, self.q, browser.couch.pki)
        self.app.state = 'info'
        self.httpd = make_server(self.app, '0.0.0.0', server_config)
        env = {'url': peer.url, 'ssl': client_config}
        self.client = CouchBase(env)
        self.httpd.start()
        self.ui = Popen([dmedia_peer_gtk, peer.id])

    def free(self):
        self.httpd.shutdown()
        del self.httpd
        self.ui.terminate()
        self.ui.wait()

    def get_secret(self, peer_id):
        if peer_id != self.peer.id:
            return False
        GLib.idle_add(self.init_secret)
        return True

    def init_secret(self):
        secret = self.cr.get_secret()
        self.browser.service.DisplaySecret(secret, False)

    def monitor_response(self):
        while True:
            signal = self.q.get()
            if signal == 'wrong_response':
                GLib.idle_add(self.retry)
            elif signal == 'response_ok':
                GLib.timeout_add(500, self.on_response_ok)
                break

    def monitor_cert_request(self):
        status = self.q.get()
        if status != 'cert_issued':
            log.error('Bad cert request from %r', self.peer)
            log.warning('Possible malicious peer: %r', self.peer)
        GLib.idle_add(self.on_cert_request, status)

    def retry(self):
        self.httpd.shutdown()
        secret = self.cr.get_secret()
        self.app.state = 'ready'
        self.httpd.start()
        self.browser.service.DisplaySecret(secret, True)

    def on_response_ok(self):
        assert self.app.state == 'response_ok'
        start_thread(self.counter_challenge)

    def counter_challenge(self):
        log.info('Getting counter-challenge from %r', self.peer)
        challenge = self.client.get('challenge')['challenge']
        (nonce, response) = self.cr.create_response(challenge)
        obj = {'nonce': nonce, 'response': response}
        log.info('Posting counter-response to %r', self.peer)
        try:
            r = self.client.post(obj, 'response')
            log.info('Counter-response accepted')
            GLib.idle_add(self.on_counter_response_ok)
        except Unauthorized:
            log.error('Counter-response rejected!')
            log.warning('Possible malicious peer: %r', self.peer)
            GLib.idle_add(self.on_counter_response_fail)

    def on_counter_response_ok(self):
        assert self.app.state == 'response_ok'
        self.app.state = 'counter_response_ok'
        start_thread(self.monitor_cert_request)

    def on_counter_response_fail(self):
        self.browser.service.Message(_('Very Bad Things!'))

    def on_cert_request(self, status):
        if status == 'cert_issued':
            self.browser.session_complete()
        else:
            self.browser.service.Message(_('Security Problems in CSR!'))


class Browser:
    def __init__(self, service, couch):
        self.service = service
        self.couch = couch
        self.avahi = AvahiPeer(couch.pki)
        self.avahi.connect('offer', self.on_offer)
        self.avahi.connect('retract', self.on_retract)
        self.avahi.browse()
        self.notifymanager = NotifyManager()
        self.indicator = None
        self.session = None

    def __del__(self):
        self.free()

    def free(self):
        if self.avahi is not None:
            self.avahi.unpublish()
            self.avahi = None
            del self.service
            del self.couch
            if self.session is not None:
                self.session.free()
                self.session = None

    def get_secret(self, peer_id):
        if self.session is None:
            return False
        return self.session.get_secret(peer_id)

    def on_offer(self, avahi, info):
        assert self.indicator is None
        self.indicator = AppIndicator3.Indicator.new(
            'dmedia-peer',
            'indicator-dmedia-peer',
            AppIndicator3.IndicatorCategory.APPLICATION_STATUS
        )
        menu = Gtk.Menu()
        accept = Gtk.MenuItem()
        accept.set_label(_('Accept {}@{}').format(info.name, info.host))
        accept.connect('activate', self.on_accept, info)
        menu.append(accept)
        menu.show_all()
        self.indicator.set_menu(menu)
        self.indicator.set_status(AppIndicator3.IndicatorStatus.ATTENTION)
        self.notifymanager.replace(
            _('Dmedia Peering Offer'),
            '{}@{}'.format(info.name, info.host),
        )

    def on_retract(self, avahi):
        if self.indicator is not None:
            self.indicator = None
            self.notifymanager.replace(_('Peering Offer Removed'))

    def on_accept(self, menuitem, info):
        assert self.session is None
        self.avahi.activate(info.id)
        self.indicator = None
        self.session = Session(self, info,
            self.avahi.get_server_config(),
            self.avahi.get_client_config()
        )
        self.avahi.publish(self.session.httpd.port)

    def cancel(self, peer_id):
        if self.avahi.deactivate(peer_id):
            self.avahi.unpublish() 
            self.session.free()
            self.session = None

    def session_complete(self):
        log.info('Browser.session_complete()')
        self.service.PeeringDone()
        self.avahi.deactivate(self.session.peer.id)
        self.avahi.unpublish() 
        self.session.free()
        self.session = None


class Publisher:
    def __init__(self, service, couch):
        self.service = service
        self.couch = couch
        self.thread = None
        self.avahi = None

    def __del__(self):
        self.free()

    def free(self):
        if self.avahi is not None:
            self.avahi.unpublish()
            self.avahi = None
            del self.service
            del self.couch

    def run(self):
        self.couch.load_pki()
        self.avahi = AvahiPeer(self.couch.pki, client_mode=True)
        self.avahi.connect('accept', self.on_accept)
        app = InfoApp(self.avahi.id)
        self.httpd = make_server(app, '0.0.0.0',
            self.avahi.get_server_config()
        )
        self.httpd.start()
        self.avahi.browse()
        self.avahi.publish(self.httpd.port)

    def on_accept(self, avahi, peer):
        log.info('Publisher.on_accept()')
        self.service.Accept()
        self.avahi.activate(peer.id)
        self.peer = peer
        self.cr = ChallengeResponse(avahi.id, peer.id)
        self.q = Queue()
        self.app = ClientApp(self.cr, self.q)
        # Reconfigure HTTPD to only accept connections from bound peer
        self.httpd.reconfigure(self.app, avahi.get_server_config())
        env = {'url': peer.url, 'ssl': avahi.get_client_config()}
        self.client = CouchBase(env)
        avahi.unpublish()

    def set_secret(self, secret):
        if self.thread is not None:
            return False
        self.cr.set_secret(secret)
        self.thread = start_thread(self.challenge)
        self.service.Message(_('Challenge...'))
        return True

    def challenge(self):
        log.info('Getting challenge from %r', self.peer)
        challenge = self.client.get('challenge')['challenge']
        (nonce, response) = self.cr.create_response(challenge)
        obj = {'nonce': nonce, 'response': response}
        log.info('Posting response to %r', self.peer)
        try:
            r = self.client.post(obj, 'response')
            log.info('Response accepted')
            success = True
        except Unauthorized:
            log.info('Response rejected')
            success = False
        GLib.idle_add(self.on_response, success)

    def on_response(self, success):
        self.thread.join()
        self.thread = None
        if success:
            self.app.state = 'ready'
            self.thread = start_thread(self.monitor_counter_response)
            self.service.Message(_('Counter-Challenge...'))
        else:
            self.service.Message(_('Typo? Please try again with new secret.'))
        self.service.Response(success)

    def monitor_counter_response(self):
        # FIXME: Should use a timeout with queue.get()
        status = self.q.get()
        log.info('Counter-response gave %r', status)
        if status != 'response_ok':
            log.error('Wrong counter-response!')
            log.warning('Possible malicious peer: %r', self.peer)
        GLib.timeout_add(500, self.on_counter_response, status)

    def on_counter_response(self, status):
        self.thread.join()
        self.thread = None
        assert self.app.state == status
        if status == 'response_ok':
            self.thread = start_thread(self.request_cert)
            self.service.Message(_('Requesting Certificate...'))
        else:
            self.service.Message(_('Scary! Counter-Challenge Failed!'))

    def request_cert(self):
        log.info('Creating CSR')
        success = False
        try:
            self.couch.pki.create_csr(self.cr.id)
            csr_data = self.couch.pki.read_csr(self.cr.id)
            obj = {
                'csr': b64encode(csr_data).decode('utf-8'),
                'mac': self.cr.csr_mac(csr_data),
            }
            d = self.client.post(obj, 'csr')
            cert_data = b64decode(d['cert'].encode('utf-8'))
            self.cr.check_cert_mac(cert_data, d['mac'])
            # Small delay to help with clocks being out of sync:
            time.sleep(1)
            self.couch.pki.write_cert(self.cr.id, self.cr.peer_id, cert_data)
            self.couch.pki.verify_cert(self.cr.id, self.cr.peer_id)
            key_data = b64decode(d['key'].encode('utf-8'))
            self.couch.pki.write_key(self.cr.peer_id, key_data)
            self.couch.pki.verify_key(self.cr.peer_id)
            success = True
        except Exception as e:
            log.exception('Could not request cert')
        GLib.idle_add(self.on_csr_response, success)

    def on_csr_response(self, success):
        self.thread.join()
        self.thread = None
        if success:
            self.service.set_user(self.cr.peer_id)
        else:
            self.service.Message(_('Scary! Certificate Request Failed!'))
