#!/usr/bin/python3

from microfiber import dc3_env
from filestore import FileStore

from dmedia.core import Core, start_file_server
from dmedia.tests.base import TempDir
from dmedia.client import DownloadWriter, HTTPClient, response_iter, DownloadComplete
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
    while True:
        try:
            (start, stop) = dw.next_slice()
            response = client.get(ch, start, stop)
            for leaf in response_iter(response, start=start):
                print(leaf.index, dw.write_leaf(leaf))
        except DownloadComplete:
            break
    dst.remove(ch.id)  # So we don't fill up /tmp
        
