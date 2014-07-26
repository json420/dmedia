#!/usr/bin/python3

from microfiber import Database, dmedia_env, dumps


def build_docs(rows):
    docs = []
    for row in rows:
        docs.extend(
            {'_id': row['id'], '_rev': rev, '_deleted': True}
            for rev in row['value']
        )
    return docs


def get_rows(db):
    kw = {
        'descending': True,
        'limit': 25,
    }
    return db.view('doc', 'conflicts', **kw)['rows']


def get_conflicts(db):
    rows = get_rows(db)
    return build_docs(rows)


def purge_conflicts(db):
    while True:
        docs = get_conflicts(db)
        print(dumps(docs, True))
        if not docs:
            break
        db.post({'docs': docs, 'all_or_nothing': True}, '_bulk_docs')


db = Database('dmedia-1', dmedia_env())
purge_conflicts(db)

