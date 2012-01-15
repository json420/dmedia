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

import microfiber

from dmedia import schema, views


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
    


