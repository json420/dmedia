#!/usr/bin/python3

import time
import logging

from usercouch.misc import TempCouch
from microfiber import Database, random_id
from filestore import DIGEST_BYTES
from dmedia.util import get_db
from dmedia.metastore import MetaStore, BufferedSave, TimeDelta

logging.basicConfig(level=logging.DEBUG)

couch = TempCouch()
env = couch.bootstrap()
db= get_db(env, True)
log_db = db.database('log-1')
log_db.ensure()
ms = MetaStore(db, log_db)

store_id1 = random_id()
store_id2 = random_id()
store_id3 = random_id()

count = 15 * 1000
buf = BufferedSave(db, 100)
print('Saving {} docs...'.format(count))
for i in range(count):
    doc = {
        '_id': random_id(DIGEST_BYTES),
        'time': time.time(),
        'type': 'dmedia/file',
        'origin': 'user',
        'atime': int(time.time()),
        'bytes': 12345678,
        'stored': {
            store_id1: {
                'copies': 1,
                'mtime': int(time.time()),
            },
            store_id2: {
                'copies': 2,
                'mtime': int(time.time()),
            },
            store_id3: {
                'copies': 1,
                'mtime': int(time.time()),
            },
        },
    }
    buf.save(doc)
buf.flush()

t = TimeDelta()
ms.purge_all()
print('Rate: {} per second'.format(count * 3 // t.delta))
print('')

