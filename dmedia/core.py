# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#
# dmedia: distributed media library
# Copyright (C) 2011 Jason Gerard DeRose <jderose@novacut.com>
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under the
# terms of the GNU Affero General Public License as published by the Free
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

"""
Core dmedia entry-point/API - start here!

For background, please see:

    https://bugs.launchpad.net/dmedia/+bug/753260


Security note on /dmedia/_local/dmedia
======================================

When `DMedia.init_filestores()` is called, it creates `FileStore` instances
based solely on information in the non-replicated /dmedia/_local/dmedia
document... despite the fact that the exact same information is also available
in the corresponding 'dmedia/store' documents.

When it comes to deciding what files dmedia will read and write, it's prudent to
assume that replicated documents are untrustworthy.

The dangerous approach would be to use a view to get all the 'dmedia/store'
documents with a machine_id that matches this machine_id, and initialize those `FileStore`.  But the problem is that if an attacker gained control of just one
of your replicating peers or services, they could insert arbitrary
'dmedia/store' documents, and have dmedia happily initialize `FileStore` at
those mount points.  And that would be "a bad thing".

So although the corresponding 'dmedia/store' records are created (if they don't
already exists), they are completely ignored when it comes to deciding what
filestores and mount points are configured.
"""

import logging
from copy import deepcopy
import os
from os import path

from couchdb import ResourceNotFound, ResourceConflict
try:
    import desktopcouch
    from desktopcouch.application.platform import find_port
    from desktopcouch.application.local_files import get_oauth_tokens
except ImportError:
    desktopcouch = None

try:
    from dmedia.webui.app import App
except ImportError:
    App = None

from .filestore import FileStore
from .constants import DBNAME
from .transfers import TransferManager
from .abstractcouch import get_server, get_db, load_env
from .schema import random_id, create_machine, create_store
from .views import init_views

try:
    from . import backends
except ImportError:
    pass


LOCAL_ID = '_local/dmedia'


log = logging.getLogger()


def get_env(dbname=DBNAME, no_dc=False):
    """
    Return default CouchDB environment.

    This will return an appropriate environment based on whether desktopcouch is
    available.  If you supply ``no_dc=True``, the environment for the default
    system wide CouchDB will be returned, even if desktopcouch is available.

    For example:

    >>> get_env(no_dc=True)
    {'url': 'http://localhost:5984/', 'dbname': 'dmedia', 'port': 5984}
    >>> get_env(dbname='foo', no_dc=True)
    {'url': 'http://localhost:5984/', 'dbname': 'foo', 'port': 5984}

    Not a perfect solution, but works for now.
    """
    if desktopcouch is None or no_dc:
        return {
            'dbname': dbname,
            'port': 5984,
            'url': 'http://localhost:5984/',
        }
    port = find_port()
    return {
        'dbname': dbname,
        'port': port,
        'url': 'http://localhost:%d/' % port,
        'oauth': get_oauth_tokens(),
    }


class LocalStores(object):
    def by_id(self, _id):
        pass

    def by_path(self, parentdir):
        pass

    def by_device(self, device):
        pass


class Core(object):
    def __init__(self, dbname=DBNAME, no_dc=False, env_s=None, callback=None):
        if env_s:
            self.env = load_env(env_s)
        else:
            self.env = get_env(dbname, no_dc)
        self.home = path.abspath(os.environ['HOME'])
        if not path.isdir(self.home):
            raise ValueError('HOME is not a dir: {!}'.format(self.home))
        self.server = get_server(self.env)
        self.db = get_db(self.env, self.server)
        self._has_app = None
        self.manager = TransferManager(self.env, callback)
        self._filestores = {}

    def bootstrap(self):
        (self.local, self.machine) = self.init_local()
        self.machine_id = self.machine['_id']
        self.env['machine_id'] = self.machine_id
        self.env['filestore'] = self.init_filestores()
        init_views(self.db)

    def init_local(self):
        """
        Get the /dmedia/_local/dmedia document, creating it if needed.
        """
        try:
            local = self.db[LOCAL_ID]
        except ResourceNotFound:
            machine = create_machine()
            local = {
                '_id': LOCAL_ID,
                'machine': deepcopy(machine),
                'filestores': {},
            }
            self.db.save(local)
            self.db.save(machine)
        else:
            try:
                machine = self.db[local['machine']['_id']]
            except ResourceNotFound:
                machine = deepcopy(local['machine'])
                self.db.save(machine)
        return (local, machine)

    def init_filestores(self):
        if not self.local['filestores']:
            self.add_filestore(self.home)
        else:
            for (parentdir, store) in self.local['filestores'].iteritems():
                assert store['path'] == parentdir
                try:
                    self.init_filestore(store)
                except Exception:
                    pass
                try:
                    self.db.save(deepcopy(store))
                except ResourceConflict:
                    pass
            if self.local.get('default_filestore') not in self.local['filestores']:
                self.local['default_filestore'] = store['path']
        return self.local['filestores'][self.local['default_filestore']]

    def init_filestore(self, store):
        parentdir = store['path']
        self._filestores[parentdir] = FileStore(parentdir)

    def add_filestore(self, parentdir):
        parentdir = path.abspath(parentdir)
        if not path.isdir(parentdir):
            raise ValueError('Not a directory: {!r}'.format(parentdir))
        try:
            return self.local['filestores'][parentdir]
        except KeyError:
            pass
        log.info('Added filestore at %r', parentdir)
        store = create_store(parentdir, self.machine_id)
        self.init_filestore(store)
        self.local['filestores'][parentdir] = deepcopy(store)
        self.local['default_filestore'] = store['path']
        self.db.save(self.local)
        self.db.save(store)
        return store

    def init_app(self):
        if App is None:
            log.info('init_app(): `dmedia.webui.app` not available')
            return False
        log.info('init_app(): creating /dmedia/app document')
        doc = App().get_doc()
        _id = doc['_id']
        assert '_rev' not in doc
        try:
            old = self.db[_id]
            doc['_rev'] = old['_rev']
            self.db.save(doc)
        except ResourceNotFound:
            self.db.save(doc)
        return True

    def has_app(self):
        if self._has_app is None:
            self._has_app = self.init_app()
        return self._has_app

    def get_file(self, file_id):
        doc = self.db[file_id]
        ext = doc.get('ext')
        for fs in self._filestores.itervalues():
            filename = fs.path(file_id, ext)
            if path.isfile(filename):
                return filename

    def upload(self, file_id, store_id):
        return self.manager.upload(file_id, store_id)

    def download(self, file_id, store_id):
        return self.manager.download(file_id, store_id)
