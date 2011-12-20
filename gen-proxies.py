from subprocess import check_call
import json
import time

from microfiber import Database, dc3_env
from filestore import FileStore
from dmedia.local import LocalSlave
from dmedia.core import init_filestore
from dmedia.schema import create_file

env = dc3_env()
db = Database('dmedia', env)
loc = LocalSlave(env)
(fs, fs_doc) = init_filestore('/home')

r = db.view('user', 'needsproxy')
for row in r['rows']:
    _id = row['id']
    try:
        src = loc.stat(_id).name
    except FileNotLocal:
        continue
    print(src)
    tmp_fp = fs.allocate_tmp()
    start = time.time()
    check_call(['./dmedia-transcoder', src, tmp_fp.name])
    elapsed = time.time() - start
    ch = fs.hash_and_move(tmp_fp)
    st = fs.stat(ch.id)
    stored = {
        fs.id: {
            'copies': fs.copies,
            'mtime': st.mtime,
            'plugin': 'filestore',
        }
    }
    proxy = create_file(ch.id, ch.file_size, ch.leaf_hashes, stored, 'proxy')
    proxy['proxyof'] = _id
    proxy['content_type'] = 'video/webm'
    proxy['ext'] = 'webm',
    proxy['elapsed'] = elapsed
    db.save(proxy)
    doc = db.get(_id)
    doc['proxies'] = doc.get('proxies', {})
    doc['proxies'][ch.id] = {
        'bytes': st.size,
        'content_type': 'video/webm',
        'width': 640,
        'height': 360,
    }
    db.save(doc)
    print('')
    print(json.dumps(db.get(_id), sort_keys=True, indent=4))
    print('')
    print(json.dumps(proxy, sort_keys=True, indent=4))


