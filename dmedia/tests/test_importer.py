# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#   Akshat Jain <ssj6akshat1234@gmail.com)
#
# dmedia: distributed media library
# Copyright (C) 2010 Jason Gerard DeRose <jderose@novacut.com>
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
Unit tests for `dmedia.importer` module.
"""

import os
from os import path
import hashlib
import tempfile
import shutil
import time
from base64 import b32decode, b32encode, b64encode
from unittest import TestCase
from multiprocessing import current_process
from .helpers import CouchCase, TempDir, TempHome, raises
from .helpers import DummyQueue, DummyCallback, prep_import_source
from .helpers import sample_mov, sample_thm
from .helpers import mov_hash, mov_leaves, mov_att, mov_qid
from .helpers import thm_hash, thm_leaves, thm_qid
from dmedia.errors import AmbiguousPath
from dmedia.filestore import FileStore
from dmedia.metastore import MetaStore
from dmedia.util import random_id
from dmedia import importer, schema

import desktopcouch
from desktopcouch.stop_local_couchdb import stop_couchdb


letters = 'gihdwaqoebxtcklrnsmjufyvpz'
extensions = ('png', 'jpg', 'mov')

relpaths = (
    ('a.jpg',),
    ('b.png',),
    ('c.png',),
    ('.car', 'a.jpg'), # files_iter() is breadth-first
    ('.car', 'b.png'), # and we make sure .dirs are being traversed
    ('.car', 'c.cr2'),
    ('bar', 'a.jpg'),
    ('bar', 'b.png'),
    ('bar', 'c.cr2'),
)


class test_functions(TestCase):
    def test_normalize_ext(self):
        f = importer.normalize_ext
        weird = ['._501', 'movie.mov.', '.movie.mov.', 'movie._501']
        for name in weird:
            self.assertEqual(f(name), (name, None))

    def test_scanfiles(self):
        f = importer.scanfiles
        tmp = TempDir()
        self.assertEqual(list(f(tmp.path)), [])
        somefile = tmp.touch('somefile.txt')
        self.assertEqual(list(f(somefile)), [])

        # Create files in a non-alphabetic order:
        names = []
        for (i, l) in enumerate(letters):
            ext = extensions[i % len(extensions)]
            name = '.'.join([l, ext.upper()])
            names.append(name)
            tmp.touch('subdir', name)

        got = list(f(tmp.path, extensions))
        expected = list(
            {
                'src': tmp.join('subdir', name),
                'base': tmp.join('subdir'),
                'root': name.split('.')[0],
                'doc': {
                    'name': name,
                    'ext': name.split('.')[1].lower(),
                },
            }
            for name in sorted(names)
        )
        self.assertEqual(got, expected)

    def test_files_iter(self):
        f = importer.files_iter
        tmp = TempDir()
        files = []
        for (i, args) in enumerate(relpaths):
            content = 'a' * (2 ** i)
            p = tmp.write(content, 'subdir', *args)
            files.append((p, len(content)))

        # Test when base is a file:
        for (p, s) in files:
            self.assertEqual(list(f(p)), [(p, s)])

        # Test importing from tmp.path:
        self.assertEqual(list(f(tmp.path)), files)

        # Test from tmp.path/subdir:
        subdir = tmp.join('subdir')
        self.assertEqual(list(f(subdir)), files)

        # Test that OSError propigates up:
        os.chmod(subdir, 0o000)
        e = raises(OSError, list, f(tmp.path))
        self.assertEqual(
            str(e),
            '[Errno 13] Permission denied: %r' % subdir
        )
        os.chmod(subdir, 0o700)

    def test_create_batch(self):
        f = importer.create_batch
        machine_id = random_id()
        doc = f(machine_id)

        self.assertEqual(schema.check_dmedia(doc), None)
        self.assertTrue(isinstance(doc, dict))
        self.assertEqual(
            set(doc),
            set([
                '_id',
                'type',
                'time',
                'imports',
                'machine_id',
                'stats',
            ])
        )
        _id = doc['_id']
        self.assertEqual(b32encode(b32decode(_id)), _id)
        self.assertEqual(len(_id), 24)
        self.assertEqual(doc['type'], 'dmedia/batch')
        self.assertTrue(isinstance(doc['time'], (int, float)))
        self.assertTrue(doc['time'] <= time.time())
        self.assertEqual(doc['imports'], [])
        self.assertEqual(doc['machine_id'], machine_id)
        self.assertEqual(
            doc['stats'],
            {
                'considered': {'count': 0, 'bytes': 0},
                'imported': {'count': 0, 'bytes': 0},
                'skipped': {'count': 0, 'bytes': 0},
                'empty': {'count': 0, 'bytes': 0},
                'error': {'count': 0, 'bytes': 0},
            }
        )

    def test_create_import(self):
        f = importer.create_import

        base = '/media/EOS_DIGITAL'
        batch_id = random_id()
        machine_id = random_id()

        keys = set([
            '_id',
            'type',
            'time',
            'base',
            'batch_id',
            'machine_id',
            'log',
            'stats',
        ])

        doc = f(base, batch_id=batch_id, machine_id=machine_id)
        self.assertEqual(schema.check_dmedia(doc), None)
        self.assertTrue(isinstance(doc, dict))
        self.assertEqual(set(doc), keys)

        _id = doc['_id']
        self.assertEqual(b32encode(b32decode(_id)), _id)
        self.assertEqual(len(_id), 24)

        self.assertEqual(doc['type'], 'dmedia/import')
        self.assertTrue(isinstance(doc['time'], (int, float)))
        self.assertTrue(doc['time'] <= time.time())
        self.assertEqual(doc['base'], base)
        self.assertEqual(doc['batch_id'], batch_id)
        self.assertEqual(doc['machine_id'], machine_id)

        doc = f(base)
        self.assertEqual(schema.check_dmedia(doc), None)
        self.assertEqual(set(doc), keys)
        self.assertEqual(doc['batch_id'], None)
        self.assertEqual(doc['machine_id'], None)
        self.assertEqual(
            doc['log'],
            {
                'imported': [],
                'skipped': [],
                'empty': [],
                'error': [],
            }
        )
        self.assertEqual(
            doc['stats'],
            {
                'imported': {'count': 0, 'bytes': 0},
                'skipped': {'count': 0, 'bytes': 0},
                'empty': {'count': 0, 'bytes': 0},
                'error': {'count': 0, 'bytes': 0},
            }
        )

    def test_to_dbus_stats(self):
        f = importer.to_dbus_stats
        stats = dict(
            imported={'count': 17, 'bytes': 98765},
            skipped={'count': 3, 'bytes': 12345},
        )
        result = dict(
            imported=17,
            imported_bytes=98765,
            skipped=3,
            skipped_bytes=12345,
        )
        self.assertEqual(f(stats), result)

    def test_accumulate_stats(self):
        f = importer.accumulate_stats
        accum = dict(
            imported={'count': 0, 'bytes': 0},
            skipped={'count': 0, 'bytes': 0},
        )
        stats1 = dict(
            imported={'count': 17, 'bytes': 98765},
            skipped={'count': 3, 'bytes': 12345},
        )
        stats2 = dict(
            imported={'count': 18, 'bytes': 9876},
            skipped={'count': 5, 'bytes': 1234},
        )
        f(accum, stats1)
        self.assertEqual(accum, stats1)
        f(accum, dict(stats2))
        self.assertEqual(
            accum,
            dict(
                imported={'count': 17 + 18, 'bytes': 98765 + 9876},
                skipped={'count': 3 + 5, 'bytes': 12345 + 1234},
            )
        )


class test_Importer(CouchCase):
    klass = importer.Importer
    batch_id = 'YKGHY6H5RVCDNMUBL4NLP6AU'

    def new(self, base, extract=False):
        return self.klass(self.batch_id, base, extract, dbname=self.dbname)

    def test_init(self):
        tmp = TempDir()
        inst = self.new(tmp.path, True)
        self.assertEqual(inst.batch_id, self.batch_id)
        self.assertEqual(inst.base, tmp.path)
        self.assertTrue(inst.extract is True)
        self.assertEqual(inst.home, self.home.path)
        self.assertTrue(isinstance(inst.filestore, FileStore))
        self.assertEqual(inst.filestore.base, self.home.join('.dmedia'))
        self.assertTrue(isinstance(inst.metastore, MetaStore))

        # Test with extract = False
        inst = self.new(tmp.path, False)
        self.assertTrue(inst.extract is False)

    def test_start(self):
        tmp = TempDir()
        inst = self.new(tmp.path)
        self.assertTrue(inst.doc is None)
        _id = inst.start()
        self.assertEqual(len(_id), 24)
        store = MetaStore(dbname=self.dbname)
        self.assertEqual(inst.doc, store.db[_id])
        self.assertEqual(
            set(inst.doc),
            set([
                '_id',
                '_rev',
                'type',
                'time',
                'base',
                'batch_id',
                'machine_id',
                'log',
                'stats',
            ])
        )
        self.assertEqual(inst.doc['batch_id'], self.batch_id)
        self.assertEqual(
            inst.doc['machine_id'],
            inst.metastore.machine_id
        )
        self.assertEqual(inst.doc['base'], tmp.path)
        self.assertEqual(
            inst.doc['log'],
            {
                'imported': [],
                'skipped': [],
                'empty': [],
                'error': [],
            }
        )
        self.assertEqual(
            inst.doc['stats'],
            {
                'imported': {'count': 0, 'bytes': 0},
                'skipped': {'count': 0, 'bytes': 0},
                'empty': {'count': 0, 'bytes': 0},
                'error': {'count': 0, 'bytes': 0},
            }
        )

    def test_scanfiles(self):
        tmp = TempDir()
        inst = self.new(tmp.path)
        inst.start()
        files = []
        for (i, args) in enumerate(relpaths):
            content = 'a' * (2 ** i)
            p = tmp.write(content, 'subdir', *args)
            files.append((p, len(content)))
        got = inst.scanfiles()
        self.assertEqual(got, tuple(files))
        self.assertEqual(
            inst.db[inst._id]['log']['considered'],
            [{'src': src, 'bytes': size} for (src, size) in files]
        )

    def test_import_file_private(self):
        """
        Test the `Importer._import_file()` method.
        """
        tmp = TempDir()
        inst = self.new(tmp.path)
        inst.start()

        # Test that AmbiguousPath is raised:
        traversal = '/home/foo/.dmedia/../.ssh/id_rsa'
        e = raises(AmbiguousPath, inst._import_file, traversal)
        self.assertEqual(e.pathname, traversal)
        self.assertEqual(e.abspath, '/home/foo/.ssh/id_rsa')

        # Test that IOError propagates up with missing file
        nope = tmp.join('nope.mov')
        e = raises(IOError, inst._import_file, nope)
        self.assertEqual(
            str(e),
            '[Errno 2] No such file or directory: %r' % nope
        )

        # Test that IOError propagates up with unreadable file
        nope = tmp.touch('nope.mov')
        os.chmod(nope, 0o000)
        e = raises(IOError, inst._import_file, nope)
        self.assertEqual(
            str(e),
            '[Errno 13] Permission denied: %r' % nope
        )
        os.chmod(nope, 0o600)

        src1 = tmp.copy(sample_mov, 'DCIM', '100EOS5D2', 'MVI_5751.MOV')
        src2 = tmp.copy(sample_mov, 'DCIM', '100EOS5D2', 'duplicate.MOV')

        # Test with new file
        size = path.getsize(src1)
        (action, doc) = inst._import_file(src1)

        self.assertEqual(action, 'imported')
        self.assertEqual(
            set(doc),
            set([
                '_id',
                '_rev',
                '_attachments',
                'type',
                'time',
                'bytes',
                'ext',
                'origin',
                'stored',

                'import_id',
                'mtime',
                'name',
                'dir',
                'content_type',
            ])
        )
        self.assertEqual(schema.check_dmedia_file(doc), None)

        self.assertEqual(doc['_id'], mov_hash)
        self.assertEqual(doc['_attachments'], {'leaves': mov_att})
        self.assertEqual(doc['type'], 'dmedia/file')
        self.assertTrue(doc['time'] <= time.time())
        self.assertEqual(doc['bytes'], size)
        self.assertEqual(doc['ext'], 'mov')

        self.assertEqual(doc['import_id'], inst._id)
        self.assertEqual(doc['mtime'], path.getmtime(src1))
        self.assertEqual(doc['name'], 'MVI_5751.MOV')
        self.assertEqual(doc['dir'], 'DCIM/100EOS5D2')
        self.assertEqual(doc['content_type'], 'video/quicktime')

        # Test with duplicate
        (action, doc) = inst._import_file(src2)
        self.assertEqual(action, 'skipped')
        self.assertEqual(doc, inst.db[mov_hash])

        # Test with duplicate with missing doc
        del inst.db[mov_hash]
        (action, doc) = inst._import_file(src2)
        self.assertEqual(action, 'skipped')
        self.assertEqual(doc['time'], inst.db[mov_hash]['time'])

        # Test with duplicate when doc is missing this filestore in store:
        old = inst.db[mov_hash]
        rid = random_id()
        old['stored'] = {rid: {'copies': 2, 'time': 1234567890}}
        inst.db.save(old)
        (action, doc) = inst._import_file(src2)
        fid = inst.filestore._id
        self.assertEqual(action, 'skipped')
        self.assertEqual(set(doc['stored']), set([rid, fid]))
        t = doc['stored'][fid]['time']
        self.assertEqual(
            doc['stored'],
            {
                rid: {'copies': 2, 'time': 1234567890},
                fid: {'copies': 1, 'time': t},
            }
        )
        self.assertEqual(inst.db[mov_hash]['stored'], doc['stored'])

        # Test with existing doc but missing file:
        old = inst.db[mov_hash]
        inst.filestore.remove(mov_hash, 'mov')
        (action, doc) = inst._import_file(src2)
        self.assertEqual(action, 'imported')
        self.assertEqual(doc['_rev'], old['_rev'])
        self.assertEqual(doc['time'], old['time'])
        self.assertEqual(inst.db[mov_hash], old)

        # Test with empty file:
        src3 = tmp.touch('DCIM', '100EOS5D2', 'foo.MOV')
        (action, doc) = inst._import_file(src3)
        self.assertEqual(action, 'empty')
        self.assertEqual(doc, {'mtime': path.getmtime(src3)})

    def test_import_file(self):
        """
        Test the `Importer.import_file()` method.
        """
        tmp = TempDir()
        inst = self.new(tmp.path)
        inst.start()

        self.assertEqual(inst.doc['log']['error'], [])
        self.assertEqual(inst._processed, [])

        # Test that AmbiguousPath is raised:
        nope1 = '/home/foo/.dmedia/../.ssh/id_rsa'
        abspath = '/home/foo/.ssh/id_rsa'
        (action, error1) = inst.import_file(nope1, 17)
        self.assertEqual(action, 'error')
        self.assertEqual(error1, {
            'src': nope1,
            'bytes': 17,
            'name': 'AmbiguousPath',
            'msg': '%r resolves to %r' % (nope1, abspath),
        })
        self.assertEqual(
            inst.doc['log']['error'],
            [error1]
        )
        self.assertEqual(
            inst._processed,
            [nope1]
        )

        # Test that IOError propagates up with missing file
        nope2 = tmp.join('nope.mov')
        (action, error2) = inst.import_file(nope2, 18)
        self.assertEqual(action, 'error')
        self.assertEqual(error2, {
            'src': nope2,
            'bytes': 18,
            'name': 'IOError',
            'msg': '[Errno 2] No such file or directory: %r' % nope2,
        })
        self.assertEqual(
            inst.doc['log']['error'],
            [error1, error2]
        )
        self.assertEqual(
            inst._processed,
            [nope1, nope2]
        )

        # Test that IOError propagates up with unreadable file
        nope3 = tmp.touch('nope.mov')
        os.chmod(nope3, 0o000)
        try:
            (action, error3) = inst.import_file(nope3, 19)
            self.assertEqual(action, 'error')
            self.assertEqual(error3, {
                'src': nope3,
                'bytes': 19,
                'name': 'IOError',
                'msg': '[Errno 13] Permission denied: %r' % nope3,
            })
            self.assertEqual(
                inst.doc['log']['error'],
                [error1, error2, error3]
            )
            self.assertEqual(
                inst._processed,
                [nope1, nope2, nope3]
            )
        finally:
            os.chmod(nope3, 0o600)


        # Test with new files
        src1 = tmp.copy(sample_mov, 'DCIM', '100EOS5D2', 'MVI_5751.MOV')
        src2 = tmp.copy(sample_thm, 'DCIM', '100EOS5D2', 'MVI_5751.THM')
        self.assertEqual(inst.doc['log']['imported'], [])

        (action, imported1) = inst.import_file(src1, 17)
        self.assertEqual(action, 'imported')
        self.assertEqual(imported1, {
            'src': src1,
            'id': mov_hash,
            'mtime': path.getmtime(src1),
            'bytes': path.getsize(src1),
        })
        self.assertEqual(
            inst.doc['log']['imported'],
            [imported1]
        )
        self.assertEqual(
            inst._processed,
            [nope1, nope2, nope3, src1]
        )

        (action, imported2) = inst.import_file(src2, 17)
        self.assertEqual(action, 'imported')
        self.assertEqual(imported2, {
            'src': src2,
            'id': thm_hash,
            'mtime': path.getmtime(src2),
            'bytes': path.getsize(src2),
        })
        self.assertEqual(
            inst.doc['log']['imported'],
            [imported1, imported2]
        )
        self.assertEqual(
            inst._processed,
            [nope1, nope2, nope3, src1, src2]
        )

        # Test with duplicate files
        dup1 = tmp.copy(sample_mov, 'DCIM', '100EOS5D2', 'MVI_5750.MOV')
        dup2 = tmp.copy(sample_thm, 'DCIM', '100EOS5D2', 'MVI_5750.THM')
        self.assertEqual(inst.doc['log']['skipped'], [])

        (action, skipped1) = inst.import_file(dup1, 17)
        self.assertEqual(action, 'skipped')
        self.assertEqual(skipped1, {
            'src': dup1,
            'id': mov_hash,
            'mtime': path.getmtime(dup1),
            'bytes': path.getsize(dup1),
        })
        self.assertEqual(
            inst.doc['log']['skipped'],
            [skipped1]
        )
        self.assertEqual(
            inst._processed,
            [nope1, nope2, nope3, src1, src2, dup1]
        )

        (action, skipped2) = inst.import_file(dup2, 17)
        self.assertEqual(action, 'skipped')
        self.assertEqual(skipped2, {
            'src': dup2,
            'id': thm_hash,
            'mtime': path.getmtime(dup2),
            'bytes': path.getsize(dup2),
        })
        self.assertEqual(
            inst.doc['log']['skipped'],
            [skipped1, skipped2]
        )
        self.assertEqual(
            inst._processed,
            [nope1, nope2, nope3, src1, src2, dup1, dup2]
        )

        # Test with empty files
        emp1 = tmp.touch('DCIM', '100EOS5D2', 'MVI_5759.MOV')
        emp2 = tmp.touch('DCIM', '100EOS5D2', 'MVI_5759.THM')
        self.assertEqual(inst.doc['log']['empty'], [])

        (action, empty1) = inst.import_file(emp1, 17)
        self.assertEqual(action, 'empty')
        self.assertEqual(empty1, {
            'src': emp1,
            'mtime': path.getmtime(emp1),
        })
        self.assertEqual(
            inst.doc['log']['empty'],
            [empty1]
        )
        self.assertEqual(
            inst._processed,
            [nope1, nope2, nope3, src1, src2, dup1, dup2, emp1]
        )

        (action, empty2) = inst.import_file(emp2, 17)
        self.assertEqual(action, 'empty')
        self.assertEqual(empty2, {
            'src': emp2,
            'mtime': path.getmtime(emp2),
        })
        self.assertEqual(
            inst.doc['log']['empty'],
            [empty1, empty2]
        )
        self.assertEqual(
            inst._processed,
            [nope1, nope2, nope3, src1, src2, dup1, dup2, emp1, emp2]
        )

        # Check state of log one final time
        self.assertEqual(
            inst.doc['log'],
            {
                'imported': [imported1, imported2],
                'skipped': [skipped1, skipped2],
                'empty': [empty1, empty2],
                'error': [error1, error2, error3],
            }
        )

    def test_import_all_iter(self):
        tmp = TempDir()
        inst = self.new(tmp.path)

        src1 = tmp.copy(sample_mov, 'DCIM', '100EOS5D2', 'MVI_5751.MOV')
        dup1 = tmp.copy(sample_mov, 'DCIM', '100EOS5D2', 'MVI_5752.MOV')
        src2 = tmp.copy(sample_thm, 'DCIM', '100EOS5D2', 'MVI_5751.THM')
        src3 = tmp.touch('DCIM', '100EOS5D2', 'Zar.MOV')
        src4 = tmp.touch('DCIM', '100EOS5D2', 'Zoo.MOV')


        import_id = inst.start()
        inst.scanfiles()
        items = tuple(inst.import_all_iter())
        self.assertEqual(len(items), 5)
        self.assertEqual(
            items,
            (
                (src1, 'imported'),
                (src2, 'imported'),
                (dup1, 'skipped'),
                (src3, 'empty'),
                (src4, 'empty'),
            )
        )
        self.assertEqual(inst.finalize(),
             {
                'considered': {
                    'count': 5,
                    'bytes': path.getsize(src1) * 2 + path.getsize(src2),
                },
                'imported': {
                    'count': 2,
                    'bytes': path.getsize(src1) + path.getsize(src2),
                },
                'skipped': {
                    'count': 1,
                    'bytes': path.getsize(dup1),
                },
                'empty': {'count': 2, 'bytes': 0},
                'error': {'count': 0, 'bytes': 0},
            }
        )


class test_ImportWorker(CouchCase):
    klass = importer.ImportWorker

    def test_run(self):
        q = DummyQueue()
        pid = current_process().pid

        tmp = TempDir()
        batch_id = 'YKGHY6H5RVCDNMUBL4NLP6AU'
        base = tmp.path
        inst = self.klass(q, base, (batch_id, base, False, self.dbname))

        src1 = tmp.copy(sample_mov, 'DCIM', '100EOS5D2', 'MVI_5751.MOV')
        dup1 = tmp.copy(sample_mov, 'DCIM', '100EOS5D2', 'MVI_5752.MOV')
        src2 = tmp.copy(sample_thm, 'DCIM', '100EOS5D2', 'MVI_5751.THM')

        mov_size = path.getsize(sample_mov)
        thm_size = path.getsize(sample_thm)

        inst.run()

        self.assertEqual(len(q.messages), 6)
        _id = q.messages[0]['args'][1]
        self.assertEqual(len(_id), 24)
        self.assertEqual(
            q.messages[0],
            dict(
                signal='started',
                args=(base, _id),
                worker='ImportWorker',
                pid=pid,
            )
        )
        self.assertEqual(
            q.messages[1],
            dict(
                signal='count',
                args=(base, _id, 3),
                worker='ImportWorker',
                pid=pid,
            )
        )
        self.assertEqual(q.messages[2],
            dict(
                signal='progress',
                args=(base, _id, 1, 3,
                    dict(action='imported', src=src1)
                ),
                worker='ImportWorker',
                pid=pid,
            )
        )
        self.assertEqual(q.messages[3],
            dict(
                signal='progress',
                args=(base, _id, 2, 3,
                    dict(action='imported', src=src2)
                ),
                worker='ImportWorker',
                pid=pid,
            )
        )
        self.assertEqual(q.messages[4],
            dict(
                signal='progress',
                args=(base, _id, 3, 3,
                    dict(action='skipped', src=dup1)
                ),
                worker='ImportWorker',
                pid=pid,
            )
        )
        self.assertEqual(
            q.messages[5],
            dict(
                signal='finished',
                args=(base, _id,
                    dict(
                        considered={'count': 3, 'bytes': (mov_size*2 + thm_size)},
                        imported={'count': 2, 'bytes': (mov_size + thm_size)},
                        skipped={'count': 1, 'bytes': mov_size},
                        empty={'count': 0, 'bytes': 0},
                        error={'count': 0, 'bytes': 0},
                    ),
                ),
                worker='ImportWorker',
                pid=pid,
            )
        )


class test_ImportManager(CouchCase):
    klass = importer.ImportManager

    def test_start_batch(self):
        callback = DummyCallback()
        inst = self.klass(callback, self.dbname)

        # Test that batch cannot be started when there are active workers:
        inst._workers['foo'] = 'bar'
        self.assertRaises(AssertionError, inst._start_batch)
        inst._workers.clear()

        # Test under normal conditions
        inst._completed = 17
        inst._total = 18
        inst._start_batch()
        self.assertEqual(inst._completed, 0)
        self.assertEqual(inst._total, 0)
        batch = inst._batch
        batch_id = batch['_id']
        self.assertTrue(isinstance(batch, dict))
        self.assertEqual(
            set(batch),
            set([
                '_id', '_rev',
                'type',
                'time',
                'imports',
                'machine_id',
                'stats',
            ])
        )
        self.assertEqual(batch['type'], 'dmedia/batch')
        self.assertEqual(batch['imports'], [])
        self.assertEqual(batch['machine_id'], inst.metastore.machine_id)
        self.assertEqual(inst.db[batch['_id']], batch)
        self.assertEqual(
            callback.messages,
            [
                ('BatchStarted', (batch_id,)),
            ]
        )

        # Test that batch cannot be re-started without first finishing
        self.assertRaises(AssertionError, inst._start_batch)

    def test_finish_batch(self):
        callback = DummyCallback()
        inst = self.klass(callback, self.dbname)
        batch_id = random_id()
        inst._batch = dict(
            _id=batch_id,
            stats=dict(
                imported={'count': 17, 'bytes': 98765},
                skipped={'count': 3, 'bytes': 12345},
            ),
        )

        # Make sure it checks that workers is empty
        inst._workers['foo'] = 'bar'
        self.assertRaises(AssertionError, inst._finish_batch)
        self.assertEqual(callback.messages, [])

        # Check that it fires signal correctly
        inst._workers.clear()
        inst._finish_batch()
        self.assertEqual(inst._batch, None)
        stats = dict(
            imported=17,
            imported_bytes=98765,
            skipped=3,
            skipped_bytes=12345,
        )
        self.assertEqual(
            callback.messages,
            [
                ('BatchFinished', (batch_id, stats)),
            ]
        )
        doc = inst.db[batch_id]
        self.assertEqual(
            set(doc),
            set([
                '_id',
                '_rev',
                'stats',
                'time_end',
            ])
        )
        cur = time.time()
        self.assertTrue(cur - 1 <= doc['time_end'] <= cur)

    def test_on_started(self):
        callback = DummyCallback()
        inst = self.klass(callback, self.dbname)
        self.assertEqual(callback.messages, [])
        inst._start_batch()
        batch_id = inst._batch['_id']
        self.assertEqual(inst.db[batch_id]['imports'], [])
        self.assertEqual(
            callback.messages,
            [
                ('BatchStarted', (batch_id,)),
            ]
        )

        one = TempDir()
        one_id = random_id()
        inst.on_started(one.path, one_id)
        self.assertEqual(inst.db[batch_id]['imports'], [one_id])
        self.assertEqual(
            callback.messages,
            [
                ('BatchStarted', (batch_id,)),
                ('ImportStarted', (one.path, one_id)),
            ]
        )

        two = TempDir()
        two_id = random_id()
        inst.on_started(two.path, two_id)
        self.assertEqual(inst.db[batch_id]['imports'], [one_id, two_id])
        self.assertEqual(
            callback.messages,
            [
                ('BatchStarted', (batch_id,)),
                ('ImportStarted', (one.path, one_id)),
                ('ImportStarted', (two.path, two_id)),
            ]
        )

    def test_on_count(self):
        callback = DummyCallback()
        inst = self.klass(callback, self.dbname)
        self.assertEqual(callback.messages, [])

        one = TempDir()
        one_id = random_id()
        self.assertEqual(inst._total, 0)
        inst.on_count(one.path, one_id, 378)
        self.assertEqual(inst._total, 378)
        self.assertEqual(
            callback.messages,
            [
                ('ImportCount', (one.path, one_id, 378)),
            ]
        )

        two = TempDir()
        two_id = random_id()
        self.assertEqual(inst._total, 378)
        inst.on_count(two.path, two_id, 17)
        self.assertEqual(inst._total, 395)
        self.assertEqual(
            callback.messages,
            [
                ('ImportCount', (one.path, one_id, 378)),
                ('ImportCount', (two.path, two_id, 17)),
            ]
        )

    def test_on_progress(self):
        callback = DummyCallback()
        inst = self.klass(callback, self.dbname)
        self.assertEqual(callback.messages, [])

        one = TempDir()
        one_id = random_id()
        one_info = dict(
            src=one.join('happy.mov'),
            _id=mov_hash,
            action='imported',
        )
        self.assertEqual(inst._completed, 0)
        inst.on_progress(one.path, one_id, 1, 18, one_info)
        self.assertEqual(inst._completed, 1)
        self.assertEqual(
            callback.messages,
            [
                ('ImportProgress', (one.path, one_id, 1, 18, one_info)),
            ]
        )

        two = TempDir()
        two_id = random_id()
        two_info = dict(
            src=two.join('happy.thm'),
            _id='BKSTXEA5MI5DZTUDIHLI3KM3',
            action='imported',
        )
        self.assertEqual(inst._completed, 1)
        inst.on_progress(two.path, two_id, 2, 18, two_info)
        self.assertEqual(inst._completed, 2)
        self.assertEqual(
            callback.messages,
            [
                ('ImportProgress', (one.path, one_id, 1, 18, one_info)),
                ('ImportProgress', (two.path, two_id, 2, 18, two_info)),
            ]
        )

    def test_on_finished(self):
        callback = DummyCallback()
        inst = self.klass(callback, self.dbname)
        batch_id = random_id()
        inst._batch = dict(
            _id=batch_id,
            stats=dict(
                imported={'count': 0, 'bytes': 0},
                skipped={'count': 0, 'bytes': 0},
            ),
        )

        # Call with first import
        one = TempDir()
        one_id = random_id()
        one_stats = dict(
            imported={'count': 17, 'bytes': 98765},
            skipped={'count': 3, 'bytes': 12345},
        )
        inst.on_finished(one.path, one_id, one_stats)
        self.assertEqual(
            callback.messages,
            [
                ('ImportFinished', (one.path, one_id, dict(
                        imported=17,
                        imported_bytes=98765,
                        skipped=3,
                        skipped_bytes=12345,
                    ))
                ),
            ]
        )
        self.assertEqual(
            set(inst._batch),
            set(['_id', '_rev', 'stats'])
        )
        self.assertEqual(inst._batch['_id'], batch_id)
        self.assertEqual(
            inst._batch['stats']['imported'],
            {'count': 17, 'bytes': 98765}
        )
        self.assertEqual(
            inst._batch['stats']['skipped'],
            {'count': 3, 'bytes': 12345}
        )

        # Call with second import
        two = TempDir()
        two_id = random_id()
        two_stats = dict(
            imported={'count': 18, 'bytes': 9876},
            skipped={'count': 5, 'bytes': 1234},
        )
        inst.on_finished(two.path, two_id, two_stats)
        self.assertEqual(
            callback.messages,
            [
                ('ImportFinished', (one.path, one_id, dict(
                        imported=17,
                        imported_bytes=98765,
                        skipped=3,
                        skipped_bytes=12345,
                    ))
                ),
                ('ImportFinished', (two.path, two_id, dict(
                        imported=18,
                        imported_bytes=9876,
                        skipped=5,
                        skipped_bytes=1234,
                    ))
                ),
            ]
        )
        self.assertEqual(
            set(inst._batch),
            set(['_id', '_rev', 'stats'])
        )
        self.assertEqual(inst._batch['_id'], batch_id)
        self.assertEqual(
            inst._batch['stats']['imported'],
            {'count': 17 + 18, 'bytes': 98765 + 9876}
        )
        self.assertEqual(
            inst._batch['stats']['skipped'],
            {'count': 3 + 5, 'bytes': 12345 + 1234}
        )

    def test_get_batch_progress(self):
        inst = self.klass(dbname=self.dbname)
        self.assertEqual(inst.get_batch_progress(), (0, 0))
        inst._total = 18
        self.assertEqual(inst.get_batch_progress(), (0, 18))
        inst._completed = 17
        self.assertEqual(inst.get_batch_progress(), (17, 18))
        inst._completed = 0
        inst._total = 0
        self.assertEqual(inst.get_batch_progress(), (0, 0))

    def test_start_import(self):
        callback = DummyCallback()
        inst = self.klass(callback, self.dbname)
        self.assertTrue(inst.start())

        tmp = TempDir()
        (src1, src2, dup1) = prep_import_source(tmp)
        base = tmp.path
        mov_size = path.getsize(sample_mov)
        thm_size = path.getsize(sample_thm)

        # Test that False is returned when key is present
        inst._workers[base] = 'foo'
        self.assertTrue(inst.start_import(base, False) is False)

        # Now do the real thing
        inst._workers.clear()
        self.assertEqual(callback.messages, [])
        self.assertTrue(inst.start_import(base, False) is True)
        while inst._workers:
            time.sleep(1)

        self.assertEqual(len(callback.messages), 8)
        batch_id = callback.messages[0][1][0]
        import_id = callback.messages[1][1][1]
        self.assertEqual(
            callback.messages[0],
            ('BatchStarted', (batch_id,))
        )
        self.assertEqual(
            callback.messages[1],
            ('ImportStarted', (base, import_id))
        )
        self.assertEqual(
            callback.messages[2],
            ('ImportCount', (base, import_id, 3))
        )
        self.assertEqual(
            callback.messages[3],
            ('ImportProgress', (base, import_id, 1, 3,
                    dict(action='imported', src=src1)
                )
            )
        )
        self.assertEqual(
            callback.messages[4],
            ('ImportProgress', (base, import_id, 2, 3,
                    dict(action='imported', src=src2)
                )
            )
        )
        self.assertEqual(
            callback.messages[5],
            ('ImportProgress', (base, import_id, 3, 3,
                    dict(action='skipped', src=dup1)
                )
            )
        )
        self.assertEqual(
            callback.messages[6],
            ('ImportFinished', (base, import_id,
                    dict(
                        imported=2,
                        imported_bytes=(mov_size + thm_size),
                        skipped=1,
                        skipped_bytes=mov_size,
                    )
                )
            )
        )
        self.assertEqual(
            callback.messages[7],
            ('BatchFinished', (batch_id,
                    dict(
                        imported=2,
                        imported_bytes=(mov_size + thm_size),
                        skipped=1,
                        skipped_bytes=mov_size,
                    )
                )
            )
        )

    def test_list_imports(self):
        inst = self.klass(dbname=self.dbname)
        self.assertEqual(inst.list_imports(), [])
        inst._workers.update(
            dict(foo=None, bar=None, baz=None)
        )
        self.assertEqual(inst.list_imports(), ['bar', 'baz', 'foo'])
        inst._workers.clear()
        self.assertEqual(inst.list_imports(), [])
