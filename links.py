#!/usr/bin/python3

import os
from os import path
import json

from microfiber import Server

from dmedia.service import get_proxy
from dmedia.core import projects_iter



Dmedia = get_proxy()
env = json.loads(Dmedia.GetEnv())
server = Server(env)
db = server.database('dmedia-1')

home = path.abspath(os.environ['HOME'])
base = path.join(home, 'Dmedia')


def mkdir(d):
    try:
        os.mkdir(d)
        print('Created directory {!r}'.format(d))
    except FileExistsError:
        pass
    assert path.isdir(d)


mkdir(base)
for (project_db_name, project_id) in projects_iter(server):
    print(project_id)
    project_dir = path.join(base, project_id)
    mkdir(project_dir)
    project_db = server.database(project_db_name)
    kw = {
        'key': 'dmedia/file',
        'include_docs': True,
        'limit': 50,
        'skip': 0,
    }
    while True:
        rows = project_db.view('doc', 'type', **kw)['rows']
        if not rows:
            break
        kw['skip'] += len(rows)
        for row in rows:
            doc = row['doc']
            dst = path.join(project_dir, doc['name'])
            if path.exists(dst):
                continue
            (file_id, status, filename) = Dmedia.Resolve(doc['_id'])
            if status != 0:
                print('Not available: {!r}'.format(doc['_id']))
                continue
            print(dst)
            os.link(filename, dst)
            
            
