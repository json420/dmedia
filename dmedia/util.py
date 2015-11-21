# dmedia: distributed media library
# Copyright (C) 2011 Novacut Inc
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under
# the terms of the GNU Affero General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# `dmedia` is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Affero General Public License for more
# details.
#
# You should have received a copy of the GNU Affero General Public License along
# with `dmedia`.  If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>

"""
A few handy utility functions.
"""

from os import path
from copy import deepcopy
import logging

import microfiber
from filestore import DOTNAME

from . import schema, views


log = logging.getLogger()


def isfilestore(parentdir):
    return path.isdir(path.join(parentdir, DOTNAME))


def is_v1_filestore(parentdir):
    return path.isfile(path.join(parentdir, DOTNAME, 'filestore.json'))


def get_designs(db):
    rows = db.get('_all_docs', startkey='_design/', endkey='_design0')['rows']
    return dict(
        (row['id'], row['value']['rev']) for row in rows
    )


def update_design_doc(db, doc):
    assert '_rev' not in doc
    doc = deepcopy(doc)
    try:
        old = db.get(doc['_id'])
        doc['_rev'] = old['_rev']
        if doc != old:
            db.save(doc)
            return 'changed'
        else:
            return 'same'
    except microfiber.NotFound:
        db.save(doc)
        return 'new'


def init_views(db, designs):
    log.info('Initializing views in %s', db.name)
    result = []
    current = set()
    for doc in designs:
        action = update_design_doc(db, doc)
        _id = doc['_id']
        result.append((action, _id))
        current.add(_id)
    for (_id, rev) in get_designs(db).items():
        if _id not in current:
            log.info('Deleting unused %r in %r', _id, db)
            db.delete(_id, rev=rev)
            result.append(('deleted', _id))
    # Cleanup old view files:
    db.post(None, '_view_cleanup')
    return result


def init_project_views(db):  
    init_views(db, views.project)


def get_db(env, init=False):
    db = microfiber.Database(schema.DB_NAME, env)
    if init:
        db.ensure()
        init_views(db, views.core)
    return db


def get_project_db(_id, env, init=False):
    db = microfiber.Database(schema.project_db_name(_id), env)
    if init:
        db.ensure()
        init_project_views(db)
    return db
    


