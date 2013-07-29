#!/usr/bin/python3

# Test to measure latency between posting a doc to CouchDB and getting it back
# through the changes feed.

from http.client import ResponseNotReady
import time

from usercouch.misc import TempCouch
from microfiber import Database, dmedia_env, dumps


db = Database('dmedia-1', dmedia_env())

#initial = []
#for row in db.get('_all_docs')['rows']:
#    _id = row['id']
#    if _id.startswith('_'):
#        continue
#    doc = db.get(_id, attachments=True)
#    del doc['_rev']
#    print(dumps(doc, True))
#    initial.append(doc)

#fp = open('replay-initial.json', 'w')
#fp.write(dumps(initial, True))


changes = []
kw = {
    'feed': 'longpoll',
    'include_docs': True,
    'since': db.get()['update_seq']
}
start = time.monotonic()
while time.monotonic() - start < 180:
    try:
        r = db.get('_changes', **kw)
        for row in r['results']:
            doc = row['doc']
            if doc.get('type') not in ('dmedia/file', 'dmedia/machine'):
                continue
            doc.pop('_attachments', None)
            del doc['_rev']
            print(dumps(doc, pretty=True))
            changes.append(doc)
        kw['since'] = r['last_seq']
    except ResponseNotReady:
        pass


fp = open('replay-changes.json', 'w')
fp.write(dumps(changes, True))
