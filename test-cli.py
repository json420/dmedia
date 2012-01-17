#!/usr/bin/python3

from usercouch.misc import TempCouch
from microfiber import random_id
import json
import time
from subprocess import Popen, check_output

bus = 'tmp' + random_id() + '.Dmedia'

def call(*args):
    cmd = ('./dmedia-cli', '--bus', bus) + args
    print(check_output(cmd).decode('utf-8'))

# Check that printing help doesn't connect to the DBus service:
call()

# Start a tmp couchdb and the dbus service on a random bus name:
tmpcouch = TempCouch()
env = tmpcouch.bootstrap()
cmd = ['./dmedia-service', '--bus', bus, '--env', json.dumps(env)]
child = Popen(cmd)
time.sleep(1)

try:
    call('Version')
    call('GetEnv')
    call('LocalDmedia')
    call('LocalPeers')
    call('RemoveFileStore', '/home/')
    call('AddFileStore', '/home/')
finally:
    call('Kill')
