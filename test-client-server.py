#!/usr/bin/python3

import sys
import json
from os import path

import dbus
from microfiber import Database, CouchBase, build_ssl_context, dumps
from filestore import LEAF_SIZE

import dmedia
from dmedia.client import threaded_response_iter


Dmedia = dbus.SessionBus().get_object('org.freedesktop.Dmedia', '/')
env = json.loads(Dmedia.GetEnv())
db = Database('dmedia-0', env)
peers = db.get('_local/peers')['peers']

basedir = dmedia.get_dmedia_dir()
ssldir = path.join(basedir, 'ssl')
ssl_config = {
    'ca_file': path.join(ssldir, env['user_id'] + '.ca'),
    'cert_file': path.join(ssldir, env['machine_id'] + '.cert'),
    'key_file': path.join(ssldir, env['machine_id'] + '.key'),
}
ssl_context = build_ssl_context(ssl_config)


class Client(CouchBase):
    def get(self, _id):
        return self.request('GET', ('files', _id), None)



file_id = 'DQQMPJ7IZVXUWXVGZTYLD74XU6GJ3HFPFR2XTPORKTK2CCGE'
for (machine_id, info) in peers.items():
    client_env = {
        'url': info['url'],
        'ssl': {
            'context': ssl_context,
            'check_hostname': False,
        },
    }
    client = Client(client_env)
    for leaf in threaded_response_iter(client.get(file_id)):
        print(leaf.index)
    

sys.exit()

from dmedia.tests.base import TempDir
from dmedia.client import HTTPClient, threaded_response_iter
from dmedia.client import DownloadWriter, DownloadComplete
from dmedia.local import LocalSlave



core = Core(dmedia_env())
(httpd, port) = start_file_server(core.env)

url = 'http://localhost:{}/'.format(port)
client = HTTPClient(url)
tmp = TempDir()
dst = FileStore(tmp.dir)
for row in core.db.view('doc', 'type', key='dmedia/file', reduce=False)['rows']:
    ch = core.content_hash(row['id'])
    print(ch.id)
    dw = DownloadWriter(ch, dst)
    (start, stop) = dw.next_slice()
    for i in range(stop):
        response = client.get(ch, i, i+1)
        for leaf in threaded_response_iter(response, start=i):
            print(leaf.index, dw.write_leaf(leaf))
    dw.finish()
    dst.remove(ch.id)  # So we don't fill up /tmp
        
