#!/usr/bin/python3

from microfiber import Database, dc3_env, NotFound
from dmedia.split import doc_to_core, doc_to_project, migrate
from dmedia.schema import create_project, DBNAME

env = dc3_env()


def migrate_if_needed(db):
    orig = Database('dmedia', env)
    try:
        orig.get()
    except NotFound:
        return False
    doc = create_project('Auto Migrated Project')
    db.post(doc)
    project = Database(doc['db'], db.env)
    project.put(None)
    project.post(doc)
    migrate(orig, db, project)
    return True


db = Database('dmedia-0', env)
if db.ensure():
    print(migrate_if_needed(db))
    print(db.get('_local/dmedia'))
    

        
    


