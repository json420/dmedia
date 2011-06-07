import time
import dmedia

dmedia.configure_logging('upload')

from dmedia.core import Core

c = Core()
c.bootstrap()

uds_o = 'EIJ5EVPOJSO5ZBDYANRM2XT7'

#i = 0
#for row in c.db.view('user/video', include_docs=True):
#    i += 1
#    doc = dict(row.doc)
#    if 'proxy' not in doc:
#        print 'no proxy {_id}'.format(**doc)
#        continue
#    doc = c.db.get(doc['proxy'])
#    doc['content_type'] = 'video/webm'
#    doc['name'] = 'uds-o-%04d.webm' % i
#    print doc['name']
#    c.db.save(doc)
#
#raise SystemExit()


i = 0
for row in c.db.view('user/video', include_docs=True):
    doc = dict(row.doc)
    if 'proxy' not in doc:
        print 'no proxy {_id}'.format(**doc)
        continue
    doc = c.db.get(doc['proxy'])
    if uds_o in doc['stored']:
        print 'already in uds-o bucket {_id}'.format(**doc)
        continue
    print 'will upload {_id}'.format(**doc)
    while True:
        if len(c.manager.list_jobs()) < 3:
            c.upload(doc['_id'], uds_o)
            break
        else:
            print 'waiting for an upload to finish...'
            time.sleep(5)
