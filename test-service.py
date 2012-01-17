#!/usr/bin/python3

from usercouch.misc import TempCouch
from microfiber import random_id
import json
import time
from subprocess import Popen
from dmedia.service.dbus import session

bus = 'tmp' + random_id() + '.Dmedia'
tmpcouch = TempCouch()
env = tmpcouch.bootstrap()

cmd = ['./dmedia-service', '--bus', bus, '--env', json.dumps(env)]

child = Popen(cmd)
time.sleep(2)

proxy = session.get(bus, '/', 'org.freedesktop.Dmedia')
try:
    print(proxy.Version())
    print(proxy.GetEnv())
    print(proxy.GetLocalDmedia())
    print(proxy.GetLocalPeers())
    print(proxy.RemoveFileStore('(s)', '/home'))
    print(proxy.AddFileStore('(s)', '/home'))
finally:
    print(proxy.Kill())

