#!/usr/bin/python3

from os import path
import sys
import time

from microfiber import Database, dmedia_env, NotFound, Conflict
from dmedia.util import init_filestore
from dmedia import schema
from dmedia.metastore import create_stored

parentdir = path.abspath(sys.argv[1])
fs = init_filestore(parentdir)[0]
db = Database('dmedia-0', dmedia_env())

for st in fs:
    try:
        db.get(st.id)
        print('exists:', st.id)
        continue
    except NotFound:
        pass
    ch = fs.verify(st.id)
    stored = create_stored(st.id, fs)
    doc = schema.create_file(time.time(), ch, stored)
    try:
        db.save(doc)
        print(st.id)
    except Conflict:
        print('conflict', st.id)
