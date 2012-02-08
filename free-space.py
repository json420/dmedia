#!/usr/bin/python

from microfiber import Database, dmedia_env
from dmedia.core import init_filestore
from dmedia.metastore import MetaStore
import os
from os import path

env = dmedia_env()

db = Database('dmedia-0', env)
p = Database('dmedia-0-ahgokhnaabgo4dhwtuh6d4p4', env)
ms = MetaStore(env)
fs = init_filestore('/home')[0]

def iter_copies(stored):
    for key in stored:
        if key != fs.id:
            yield stored[key]['copies']

for row in p.view('user', 'ctime')['rows']:
    _id = row['id']
    doc = db.get(_id)
    stored = doc['stored']
    if fs.id not in stored:
        print('not in home', _id)
        continue
    copies = sum(iter_copies(stored))
    if copies >= 2:
        print(copies, _id)
        ms.remove(fs, _id)
    

