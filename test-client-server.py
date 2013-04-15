#!/usr/bin/python3

import sys
import json
from os import path
import time

import dbus
from microfiber import Database, build_ssl_context, dumps
from filestore import LEAF_SIZE
from filestore.misc import TempFileStore

import dmedia
from dmedia.util import init_filestore
from dmedia.units import bytes10
from dmedia.metastore import MetaStore
from dmedia.client import HTTPClient, Downloader


Dmedia = dbus.SessionBus().get_object('org.freedesktop.Dmedia', '/')
env = json.loads(Dmedia.GetEnv())
db = Database('dmedia-0', env)
ms = MetaStore(db)
peers = db.get('_local/peers')['peers']

basedir = dmedia.get_dmedia_dir()
ssldir = path.join(basedir, 'ssl')
ssl_config = {
    'ca_file': path.join(ssldir, env['user_id'] + '.ca'),
    'cert_file': path.join(ssldir, env['machine_id'] + '.cert'),
    'key_file': path.join(ssldir, env['machine_id'] + '.key'),
}
ssl_context = build_ssl_context(ssl_config)

fs = init_filestore('/media/jderose/dmedia2')[0]
file_id = 'DDKVF5J6YJJ3WJAIDNZDDWN672MXPLTWVGVYGI7N63SRFIHV'
downloader = Downloader(file_id, ms, fs)
print(dumps(downloader.doc, True))
print(len(downloader.missing))


for (machine_id, info) in peers.items():
    client_env = {
        'url': info['url'],
        'ssl': {
            'context': ssl_context,
            'check_hostname': False,
        },
    }
    client = HTTPClient(client_env)
    start = time.monotonic()

    downloader.download_from(client)
    print(dumps(downloader.doc, True))

    delta = time.monotonic() - start
    rate = int(downloader.ch.file_size / delta)
    print(bytes10(rate))
