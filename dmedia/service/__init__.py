# dmedia: distributed media library
# Copyright (C) 2011 Novacut Inc
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under
# the terms of the GNU Affero General Public License as published by the Free
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
Functionality that requires DBus and is generally Linux-specific.

Code that is portable should go in dmedia/*.py (the dmedia core). 
"""

import logging

import dbus
from dbus.mainloop.glib import DBusGMainLoop
from gi.repository import GObject


DBusGMainLoop(set_as_default=True)
GObject.threads_init()


def get_proxy():
    session = dbus.SessionBus()
    return session.get_object('org.freedesktop.Dmedia', '/')

 
class CheckNeedsInit:
    def __init__(self):
        logging.info('Getting Dmedia proxy object...')
        Dmedia = get_proxy()
        logging.info('Checking Dmedia.NeedsInit()...')
        if Dmedia.NeedsInit():
            logging.info('Needs firstrun init.')
            self.mainloop = GObject.MainLoop()
            Dmedia.connect_to_signal('InitDone', self.on_InitDone)
            Dmedia.DoInit()
            self.mainloop.run()

    def on_InitDone(self, env_s):
        logging.info('@Dmedia.InitDone()')
        self.mainloop.quit() 

