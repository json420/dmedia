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


src = c._filestores['/home']
src_id = c.local['filestores']['/home']['_id']

i = 0
for row in c.db.view('user/video', include_docs=True, descending=True):
    doc = dict(row.doc)
    if 'proxy' in doc:
        print 'skipping {!r}'.format(doc['_id'])
        continue
    job = {
        'src': {'id': doc['_id'], 'ext': doc['ext']},
        'mux': 'oggmux',
        'video': {
            'enc': 'theoraenc',
            'props': {'quality': 40},
            'caps': {'width': 960, 'height': 540},
        },
        'audio': {
            'enc': 'vorbisenc',
            'props': {'quality': 0.3},
        },
        'ext': 'ogv',
    }
    print job
    t = Transcoder(job, src)
    (_id, leaves) = t.run()
    f = src.path(_id, 'ogv')
    proxy = create_file(path.getsize(f), leaves, src_id,
        ext='ogv',
        origin='proxy',
    )
    proxy['proxyof'] = doc['_id']
    print proxy
    doc['proxy'] = _id
    c.db.save(doc)
    c.db.save(proxy)
    os.symlink(f, '/home/jderose/Videos/uds/%03d.ogv' % i)
    i += 1

    
