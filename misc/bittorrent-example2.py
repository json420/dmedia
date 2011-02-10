from __future__ import print_function

import sys
import os
from os import path
import time
from base64 import b32decode
from dmedia.filestore import FileStore, HashList
import libtorrent


# Known size, top hash, and leaf hashes for test video:
size = 448881430

chash = '37GDNHANX7RCBMBGTYLSIK7TMTUQSKDS'

leaves_b32 = [
    'TDCPJHYQVEVCTMLIKEQITVMJKSUIETHD',
    'UAG5HQCLH6PGA4RAYXDEFRNCSZDTMLXU',
    '2DGBOSUSSDG5OKASXNQTJG3MANGL4H2U',
    'D4TMBNAQOOFMIWB2ATT2GR7Y262EATVM',
    'SEYVEQPAVCROXPYZWIXN4YZRHOZV2MWV',
    'B2I7VCLIVV4LBSRTGSRQNNXDDPWCKNLA',
]

leaves = [b32decode(l) for l in leaves_b32]


# Create a FileStore in ~/.dmedia_test/
home = path.abspath(os.environ['HOME'])
base = path.join(home, '.dmedia_test')
store = FileStore(base)

# Get tmp path we will write file to as we download:
tmp = store.temp(chash, 'tgz', create=True)
print('Will write file to:\n  %r\n' % tmp)

# Path of torret file in misc/
torrent = path.join(
    path.dirname(path.abspath(__file__)),
    'example.torrent'
)
assert path.isfile(torrent), torrent
print('Using torrent:\n  %r\n' % torrent)


session = libtorrent.session()
session.listen_on(6881, 6891)

e = libtorrent.bdecode(open(torrent, 'rb').read())
info = libtorrent.torrent_info(e)

h = session.add_torrent({
    'ti': info,
    'save_path': path.dirname(tmp),
})


while not h.is_seed():
    s = h.status()
    print(s.progress)
    time.sleep(2)


print(h.name())
