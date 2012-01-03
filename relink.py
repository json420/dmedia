#!/usr/bin/python3

import sys
from os import path

from microfiber import dmedia_env, Conflict, NotFound
from dmedia.util import get_db
from dmedia.core import init_filestore

parentdir = path.abspath(sys.argv[1])

db = get_db(dmedia_env())
(fs, doc) = init_filestore(parentdir)
try:
    db.save(doc)
except Conflict:
    pass

for st in fs:
    print(st.id)
    try:
        doc = db.get(st.id)
        doc['stored'][fs.id] = {
            'copies': 1,
            'mtime': st.mtime,
            'plugin': 'filestore',
        }
        db.save(doc)
    except NotFound:
        pass

