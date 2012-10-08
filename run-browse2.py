#!/usr/bin/python3

import logging
import tempfile

from gi.repository import GObject
from microfiber import dumps

from dmedia.startup import DmediaCouch
from dmedia.peering import ChallengeResponseApp
from dmedia.service.peers import AvahiPeer
from dmedia.httpd import WSGIError, make_server


format = [
    '%(levelname)s',
    '%(processName)s',
    '%(threadName)s',
    '%(message)s',
]
logging.basicConfig(level=logging.DEBUG, format='\t'.join(format))


mainloop = GObject.MainLoop()

couch = DmediaCouch(tempfile.mkdtemp())
couch.firstrun_init(create_user=True)
couch.load_pki()

def on_offer(avahi, info):
    app = ChallengeResponseApp(avahi.id, info.id)
    avahi.httpd = make_server(app, '0.0.0.0',
        avahi.get_server_config()
    )
    avahi.httpd.start()
    avahi.accept(info.id, avahi.httpd.port)

avahi = AvahiPeer(couch.pki)
avahi.connect('offer', on_offer)
avahi.browse()



mainloop.run()


