#!/usr/bin/python3

import sys
import json
from os import path
import time
import logging
import multiprocessing

import dbus
from filestore.misc import TempFileStore
from dbase32.rfc3548 import random_id

import dmedia
from dmedia.util import get_db
from dmedia.units import bytes10
from dmedia.client import download_worker
from dmedia.parallel import start_process


logging.basicConfig(level=logging.DEBUG)


Dmedia = dbus.SessionBus().get_object('org.freedesktop.Dmedia', '/')
env = json.loads(Dmedia.GetEnv())
db = get_db(env)
tmpfs = TempFileStore(random_id())

basedir = dmedia.get_dmedia_dir()
ssldir = path.join(basedir, 'ssl')
ssl_config = {
    'ca_file': path.join(ssldir, env['user_id'] + '.ca'),
    'cert_file': path.join(ssldir, env['machine_id'] + '.cert'),
    'key_file': path.join(ssldir, env['machine_id'] + '.key'),
}

queue = multiprocessing.Queue()
worker = start_process(download_worker, queue, env, ssl_config, tmpfs=tmpfs)

for row in db.view('doc', 'type', key='dmedia/file', limit=200)['rows']:
    _id = row['id']
    queue.put(_id)

queue.put(None)
worker.join()

