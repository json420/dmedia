#!/usr/bin/python3

import logging
import tempfile
from gettext import gettext as _

from gi.repository import GObject, Gtk, AppIndicator3, Notify
from microfiber import dumps, CouchBase
from queue import Queue

from dmedia.startup import DmediaCouch
from dmedia.peering import ChallengeResponse, ChallengeResponseApp
from dmedia.service.peers import AvahiPeer
from dmedia.httpd import WSGIError, make_server


format = [
    '%(levelname)s',
    '%(processName)s',
    '%(threadName)s',
    '%(message)s',
]
logging.basicConfig(level=logging.DEBUG, format='\t'.join(format))
Notify.init('dmedia-peer')



class Session:
    def __init__(self, _id, peer, server_config, client_config):
        self.cr = ChallengeResponse(_id, peer.id)
        self.q = Queue()
        self.app = ChallengeResponseApp(self.cr, self.q)
        self.app.state = 'info'
        self.httpd = make_server(self.app, '0.0.0.0', server_config)
        env = {'url': peer.url, 'ssl': client_config}
        self.client = CouchBase(env)
        self.httpd.start()


class Browse:
    def __init__(self):
        self.couch = DmediaCouch(tempfile.mkdtemp())
        self.couch.firstrun_init(create_user=True)
        self.couch.load_pki()
        self.avahi = AvahiPeer(self.couch.pki)
        self.avahi.connect('offer', self.on_offer)
        self.avahi.browse()

    def on_offer(self, avahi, info):
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
        self.note = Notify.Notification.new(
            _('Novacut Peering Offer'),
            '{}@{}'.format(info.name, info.host),
            None
        )
        self.note.show()

    def on_accept(self, menuitem, info):
        self.avahi.activate(info.id)
        del self.indicator
        self.session = Session(self.avahi.id, info,
            self.avahi.get_server_config(),
            self.avahi.get_client_config()
        )
        self.avahi.publish(self.session.httpd.port)


browse = Browse()
mainloop = GObject.MainLoop()
mainloop.run()

