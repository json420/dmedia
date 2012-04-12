#!/usr/bin/python

from microfiber import Database, dmedia_env
from dmedia.core import init_filestore
from dmedia.metastore import MetaStore
import os
from os import path
import sys

src_dir = path.abspath(sys.argv[1])
dst_dir = path.abspath(sys.argv[2])

env = dmedia_env()

db = Database('dmedia-0-ahgokhnaabgo4dhwtuh6d4p4', env)
ms = MetaStore(env)
src = init_filestore(src_dir)[0]
dst = init_filestore(dst_dir)[0]
print(src)
print(dst)

for row in db.view('user', 'ctime')['rows']:
    _id = row['id']
    if dst.exists(_id) or not src.exists(_id):
        continue
    print(_id)
    ms.copy(src, _id, dst)
    
