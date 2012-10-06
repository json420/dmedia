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
avahi = Peer(couch.pki)
avahi.browse()

mainloop.run()


