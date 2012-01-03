#!/usr/bin/python3

from microfiber import Database, dmedia_env
from dmedia.local import LocalSlave, FileNotLocal
from dmedia.extractor import create_thumbnail
from dmedia.units import bytes10
from dmedia.schema import DB_NAME

sizes = []
env = dmedia_env()
db = Database(DB_NAME, env)
local = LocalSlave(env)
result = db.view('user', 'ctime')
total = result['total_rows']
for (i, row) in enumerate(result['rows']):
    _id = row['id']
    doc = db.get(_id)
    try:
        st = local.stat2(doc)
    except FileNotLocal:
        continue
    ext = doc.get('ext')
    thm = create_thumbnail(st.name, ext)
    if thm is None:
        continue
    size = len(thm.data)
    sizes.append(size)
    print('{}/{} {} {} {}'.format(i + 1, total, _id, ext, bytes10(size)))
    db.put_att(thm.content_type, thm.data, _id, 'thumbnail', rev=doc['_rev'])

print('')
print('Average thumbnail size:', bytes10(sum(sizes) / len(sizes)))
    
