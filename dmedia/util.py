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

import stat
import json
import os
from os import path
from copy import deepcopy

import microfiber
from filestore import FileStore, DOTNAME

from . import schema, views


def isfilestore(parentdir):
    return path.isdir(path.join(parentdir, DOTNAME))


def get_filestore(parentdir, store_id, copies=None):
    store = path.join(parentdir, DOTNAME, 'store.json')
    doc = json.load(open(store, 'r'))
    if doc['_id'] != store_id:
        raise Exception(
            'expected store_id {!r}; got {!r}'.format(store_id, doc['_id'])
        )
    if copies is not None:
        doc['copies'] = copies
    fs = FileStore(parentdir, doc['_id'], doc['copies'])
    return (fs, doc)


def init_filestore(parentdir, copies=1):
    fs = FileStore(parentdir)
    store = path.join(fs.basedir, 'store.json')
    try:
        doc = json.load(open(store, 'r'))
    except Exception:
        doc = schema.create_filestore(copies)
        json.dump(doc, open(store, 'w'), sort_keys=True, indent=4)
        os.chmod(store, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
    fs.id = doc['_id']
    fs.copies = doc['copies']
    return (fs, doc)


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


def get_db(env, init=False):
    db = microfiber.Database(schema.DB_NAME, env)
    if init:
        db.ensure()
        views.init_views(db, views.core)
    return db


def get_project_db(_id, env, init=False):
    db = microfiber.Database(schema.project_db_name(_id), env)
    if init:
        db.ensure()
        views.init_views(db, views.project)
    return db
    


