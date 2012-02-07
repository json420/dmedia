#!/usr/bin/python

import sys
from microfiber import Database, dmedia_env
from filestore import FileStore
import os
from os import path

parentdir = path.abspath(sys.argv[1])

db = Database('dmedia-0-ahgokhnaabgo4dhwtuh6d4p4', dmedia_env())
fs = FileStore(parentdir)

rows = db.view('user', 'ctime')['rows']
for (i, row) in enumerate(rows):
    _id = row['id']
    if not fs.exists(_id):
        continue
    target = fs.path(_id)
    doc = db.get(_id)
    link = path.join(fs.parentdir, 'links', '{}.{}'.format(i, doc['ext']))
    os.symlink(target, link)
