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

for row in p.view('user', 'ctime')['rows']:
    _id = row['id']
    doc = db.get(_id)
    copies = sum(s['copies'] for s in doc['stored'].values())
    if copies > 2:
        print(copies, _id)
        ms.remove(fs, _id)
    

