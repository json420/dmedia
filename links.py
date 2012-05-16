import os
from os import path
import json
import shutil

import dbus
from filestore import FileStore

from dmedia.util import get_project_db


session = dbus.SessionBus()
Dmedia = session.get_object('org.freedesktop.Dmedia', '/')
env = json.loads(Dmedia.GetEnv())
fs = FileStore('/media/Novacut-1')

projects = ('QSD2VXLVRW4AXNKO3HZJT5TC', 'E2M4VPIIYXIYTTXVGFITTRCV')
#projects = ('E2M4VPIIYXIYTTXVGFITTRCV',)


for project_id in projects:
    db = get_project_db(project_id, env)
    p = db.get(project_id)
    print(project_id, p['title'])
    base = path.join(fs.parentdir, p['title'])
    if not path.isdir(base):
        os.mkdir(base)
    i = 0
    for row in db.view('user', 'ctime')['rows']:
        _id = row['id']
        doc = db.get(_id)
        ext = doc.get('ext')
        if ext in ('mov', 'wav', 'cr2', 'jpg'):
            i += 1
            src = fs.path(_id)
            d = path.join(base, ext)
            if not path.isdir(d):
                os.mkdir(d)
            dst = path.join(d, '{:04d}.{}'.format(i, ext))
            print(src)
            print(dst)
            os.symlink(src, dst)
            #shutil.copy2(src, dst)


#target = Dmedia.Resolve(_id)
#target = fs.path(_id)
#link = path.join('/media/NovacutSilver/links', )
#print(link)
#os.symlink(target, link) 

