#!/usr/bin/python3

import logging
import tempfile
import ssl

from gi.repository import GObject

from dmedia.startup import DmediaCouch
from dmedia.service.peers import Peer


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
user_id = couch.pki.user.id
avahi = Peer(user_id, couch.pki)
avahi.browse('_dmedia-offer._tcp')

mainloop.run()


