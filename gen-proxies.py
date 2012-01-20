from subprocess import check_call
import json
import time

from microfiber import Database, dmedia_env
from filestore import FileStore
from dmedia.local import LocalSlave
from dmedia.core import init_filestore
from dmedia.schema import create_file, DB_NAME

env = dmedia_env()
db = Database(DB_NAME, env)
loc = LocalSlave(env)
(fs, fs_doc) = init_filestore('/home')


def default_job(src, dst):
    return {
        'src': src,
        'dst': dst,
        'mux': 'webmmux',
        'video': {
            'encoder': {
                'name': 'vp8enc',
                'props': {
                    'quality': 10,
                    'max-keyframe-distance': 15,
                    'speed': 2,
                    #'threads': 2,
                },
            },
            'filter': {
                'mime': 'video/x-raw-yuv',
                'caps': {'width': 960, 'height': 540},
            },
        },
        'audio': {
            'encoder': {
                'name': 'vorbisenc',
                'props': {'quality': 0.3},
            },
        },
    }


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
    job = default_job(src, tmp_fp.name)
    check_call(['./dmedia-transcoder', json.dumps(job)])
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
    proxy['proxy_of'] = _id
    proxy['content_type'] = 'video/webm'
    proxy['ext'] = 'webm'
    proxy['elapsed'] = elapsed
    db.save(proxy)
    doc = db.get(_id)
    doc['proxies'] = doc.get('proxies', {})
    doc['proxies'][ch.id] = {
        'bytes': st.size,
        'content_type': 'video/webm',
        'width': 960,
        'height': 540,
    }
    db.save(doc)
    print('')
    print(json.dumps(db.get(_id), sort_keys=True, indent=4))
    print('')
    print(json.dumps(proxy, sort_keys=True, indent=4))


