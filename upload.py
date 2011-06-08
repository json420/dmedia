import time
import dmedia

dmedia.configure_logging('upload')

from dmedia.core import Core

c = Core()
c.bootstrap()

from dmedia.gtkui.util import units_base10

uds_o = 'EIJ5EVPOJSO5ZBDYANRM2XT7'

#for row in c.db.view('_all_docs', include_docs=True):
#    doc = dict(row.doc)
#    if doc.get('type') == 'dmedia/file' and 'mime' in doc:
#        print doc['_id']
#        doc['content_type'] = doc.pop('mime')
#        c.db.save(doc)

#raise SystemExit()

#i = 0
#for row in c.db.view('user/video', include_docs=True):
#    i += 1
#    doc = dict(row.doc)
#    name = doc['name']
#    doc['name'] = 'uds-o-%04d.mov' % i
#    #c.db.save(doc)
#    print name, doc['name']
#    continue
#    if 'proxy' not in doc:
#        print 'no proxy {_id}'.format(**doc)
#        continue
#    doc = c.db.get(doc['proxy'])
#    doc['content_type'] = 'video/webm'
#    doc['name'] = 'uds-o-%04d.webm' % i
#    print doc['name']
#    c.db.save(doc)
#raise SystemExit()


def upload(doc):
    if uds_o in doc['stored']:
        print 'already in uds-o bucket {_id}'.format(**doc)
        return
    while True:
        jobs = c.manager.list_jobs()
        if len(jobs) < 1:
            print units_base10(doc['bytes'])
            c.upload(doc['_id'], uds_o)
            return
        else:
            print '\nwaiting for an upload to finish...'
            for j in jobs:
                print j
            time.sleep(10)

i = 0
for row in c.db.view('user/video', include_docs=True):
    i += 1
    print '### %03d ###' % i
    if i == 154:
        print i, 'skipping'
        continue
    if i > 157:
        print 'all done!'
        break

    doc = dict(row.doc)
    upload(doc)

    if 'proxy' not in doc:
        print 'no proxy {_id}'.format(**doc)
        break

    proxy = c.db.get(doc['proxy'])
    print i, proxy['_id']
    upload(proxy)


while True:
    jobs = c.manager.list_jobs()
    if len(jobs) == 0:
        print 'done!'
        break
    else:
        print '\nwaiting for jobs to finish...'
        for j in jobs:
            print j
        time.sleep(10)
