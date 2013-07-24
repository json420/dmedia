#!/usr/bin/python3

from microfiber import Database, dmedia_env, NotFound
from dmedia.metastore import BufferedSave


src = Database('dmedia-1', dmedia_env())
dst = src.database('rpk-dmedia-1')

try:
    dst.delete()
except NotFound:
    pass
dst.put(None)

nope = 0
buf = BufferedSave(dst, 100)
skip = 0
while True:
    rows = src.get('_all_docs', skip=skip, limit=100)['rows']
    if not rows:
        break
    skip += len(rows)
    for row in rows:
        _id = row['id']
        #print(_id)
        doc = src.get(_id, attachments=True)
        if doc.get('type') == 'dmedia/file':
            del doc['_rev']
            buf.save(doc)
        else:
            print(_id, doc.get('type'))
            nope += 1
buf.flush()

print('{} total, {} conflicts'.format(buf.count, buf.conflicts))
print('nope', nope)
