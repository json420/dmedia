#!/usr/bin/python3

from microfiber import Database, dc3_env
from dmedia.split import migrate_if_needed


db = Database('dmedia-0', dc3_env())
if db.ensure():
    print(migrate_if_needed(db))
    print(db.get('_local/dmedia'))
    

        
    


