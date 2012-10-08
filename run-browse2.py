#!/usr/bin/python3

import logging
import tempfile
from gettext import gettext as _

from gi.repository import GObject, Gtk, AppIndicator3
from microfiber import dumps, CouchBase, random_id
from queue import Queue

from dmedia.startup import DmediaCouch
from dmedia.gtk.ubuntu import NotifyManager
from dmedia.peering import ChallengeResponse, ChallengeResponseApp
from dmedia.service.peers import AvahiPeer
from dmedia.gtk.peering import BaseUI
from dmedia.httpd import WSGIError, make_server


format = [
    '%(levelname)s',
    '%(processName)s',
    '%(threadName)s',
    '%(message)s',
]
logging.basicConfig(level=logging.DEBUG, format='\t'.join(format))


mainloop = GObject.MainLoop()



class UI(BaseUI):
    page = 'server.html'

    signals = {
        'get_secret': [],
        'display_secret': ['secret'],
    }

    def __init__(self, cr):
        super().__init__()
        self.cr = cr

    def connect_hub_signals(self, hub):
        hub.connect('get_secret', self.on_get_secret)

    def on_get_secret(self, hub):
        secret = self.cr.get_secret()
        hub.send('display_secret', secret)


class Session:
    def __init__(self, _id, peer, server_config, client_config):
        self.peer_id = peer.id
        self.cr = ChallengeResponse(_id, peer.id)
        self.q = Queue()
        self.app = ChallengeResponseApp(self.cr, self.q)
        self.app.state = 'info'
        self.httpd = make_server(self.app, '0.0.0.0', server_config)
        env = {'url': peer.url, 'ssl': client_config}
        self.client = CouchBase(env)
        self.httpd.start()
        self.ui = UI(self.cr)


class Browse:
    def __init__(self):
        self.couch = DmediaCouch(tempfile.mkdtemp())
        self.couch.firstrun_init(create_user=True)
        self.couch.load_pki()
        self.avahi = AvahiPeer(self.couch.pki)
        self.avahi.connect('offer', self.on_offer)
        self.avahi.connect('retract', self.on_retract)
        self.avahi.browse()
        self.notifymanager = NotifyManager()
        self.indicator = None
        self.session = None

    def on_offer(self, avahi, info):
        assert self.indicator is None
        self.indicator = AppIndicator3.Indicator.new(
            'dmedia-peer',
            'indicator-novacut',
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
            _('Novacut Peering Offer'),
            '{}@{}'.format(info.name, info.host),
        )

    def on_retract(self, avahi):
        if hasattr(self, 'indicator'):
            del self.indicator
            self.notifymanager.replace(_('Peering Offer Removed'))

    def on_accept(self, menuitem, info):
        assert self.session is None
        self.avahi.activate(info.id)
        self.indicator = None
        self.session = Session(self.avahi.id, info,
            self.avahi.get_server_config(),
            self.avahi.get_client_config()
        )
        self.session.ui.window.connect('destroy', self.on_destroy)
        self.session.ui.show()
        self.avahi.publish(self.session.httpd.port)

    def on_destroy(self, *args):
        self.session.httpd.shutdown()
        self.session.ui.window.destroy()
        self.avahi.deactivate(self.session.peer_id)
        self.session = None

browse = Browse()
mainloop.run()

