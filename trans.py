import dmedia
dmedia.configure_logging('transcoder')


from dmedia.core import Core
from dmedia.transcoder import Transcoder
from dmedia.schema import create_file
import time
import os
from os import path

c = Core()
c.bootstrap()


d = '/media/dmedia1'
src = c._filestores[d]
src_id = c.local['filestores'][d]['_id']

i = 0
for row in c.db.view('user/video', include_docs=True):
    i += 1
    doc = dict(row.doc)
    if 'proxy' in doc:
        print 'skipping {!r}'.format(doc['_id'])
        #del doc['proxy']
        #c.db.save(doc)
        continue
    job = {
        'src': {'id': doc['_id'], 'ext': doc['ext']},
        'mux': 'webmmux',
        'video': {
            'enc': 'vp8enc',
            'props': {'quality': 7, 'threads': 3},
            'caps': {'width': 640, 'height': 360},
        },
        'audio': {
            'enc': 'vorbisenc',
            'props': {'quality': 0.3},
            'caps': {'channels': 1},
        },
        'ext': 'webm',
    }
    print job
    t = Transcoder(job, src)
    tup = t.run()
    if tup is None:
        print 'Failed: {}'.format(doc['_id'])
        doc['fail'] = True
        c.db.save(doc)
        continue

    (_id, leaves) = tup
    f = src.path(_id, job['ext'])
    proxy = create_file(path.getsize(f), leaves, src_id,
        ext=job['ext'],
        origin='proxy',
    )
    proxy['proxyof'] = doc['_id']
    print proxy
    doc['proxy'] = _id
    c.db.save(doc)
    c.db.save(proxy)
    os.symlink(f, path.join(d, 'proxy', '%04d.ogv' % i))
