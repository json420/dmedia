#!/usr/bin/python3

from microfiber import Database, dc3_env
from dmedia.split import migrate_if_needed
from dmedia.util import get_db


env = dc3_env()
db = get_db(env, init=True)
print(migrate_if_needed(db))
print(db.get('_local/dmedia'))
    

        
    


