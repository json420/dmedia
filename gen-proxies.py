from subprocess import check_call

from microfiber import Database, dc3_env
from filestore import FileStore
from dmedia.local import LocalSlave
from dmedia.core import init_filestore
from dmedia.schema import create_file

env = dc3_env()
db = Database('dmedia', env)
loc = LocalSlave(env)
(fs, fs_doc) = init_filestore('/home')

#r = db.view('user', 'needsproxy', limit=1)
#for row in r['rows']:
#    _id = row['id']
#    src = loc.stat(_id).name
#    print(src)

_id = '24H65FJ3AQA5KMYO5LYVH5YVZEQ7SPMJ72N2JD6BHMP7AXDG'
src = loc.stat(_id).name
tmp_fp = fs.allocate_tmp()
check_call(['./dmedia-transcoder', src, tmp_fp.name])
ch = fs.hash_and_move(tmp_fp)
stored = {
    fs.id: {
        'copies': fs.copies,
        'mtime': fs.stat(ch.id).mtime,
        'plugin': 'filestore',
    }
}
proxy = create_file(ch.id, ch.file_size, ch.leaf_hashes, stored, 'proxy')
proxy['proxyof'] = _id
db.save(proxy)
doc = db.get(_id)
doc['proxies'] = doc.get('proxies', {})
doc['proxies'][ch.id] = {
    '
}


