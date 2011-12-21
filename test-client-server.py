#!/usr/bin/python3

from microfiber import dc3_env
from filestore import FileStore
import time

from dmedia.core import Core, start_file_server
from dmedia.tests.base import TempDir
from dmedia.client import HTTPClient, threaded_response_iter
from dmedia.client import DownloadWriter, DownloadComplete
from dmedia.local import LocalSlave

core = Core(dc3_env())
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
        
