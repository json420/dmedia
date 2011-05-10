from dmedia.core import Core
import time

c = Core()
c.bootstrap()


src = c._filestores['/home']
src_id = c.local['filestores']['/home']['_id']
dst = c._filestores['/media/dmedia1']
dst_id = c.local['filestores']['/media/dmedia1']['_id']

print(src)
print(src_id)
print(dst)
print(dst_id)

for row in c.db.view('user/copies', include_docs=True, key=1):
    doc = row.doc
    ext = doc.get('ext')
    assert src_id in doc['stored']
    assert dst_id not in doc['stored']
    fp = src.open(doc['_id'], ext)
    (_id, leaves) = dst.import_file(fp, ext)
    assert _id == doc['_id']
    doc['stored'][dst_id] = {
        'copies': 1,
        'time': time.time(),
    }
    print(doc['stored'])
    c.db.save(doc)
    
