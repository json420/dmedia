#!/usr/bin/python
# Authors:
#   Martin Owens <doctormo@gmail.com>
#
# Copyright  (C)  2011 Martin Owens <doctormo@gmail.com>
#
# This file is stand alone and can be used outside of 'dmedia'.
#
# This code is free software: you can redistribute it and/or modify it under the
# terms of the GNU Affero General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# This code is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Affero General Public License for more
# details.
#
# You should have received a copy of the GNU Affero General Public License along
# with this code.  If not, see <http://www.gnu.org/licenses/>.
"""
Upload Server test suite to test the protocol of resumable html5 uploads.
"""
import json
import os
import sys
import unittest
from test import test_support
from wsgiref.util import setup_testing_defaults
from dmedia.uploadserver import UploadApp, CHUNK_SIZE, UPLOAD_DIR
from base64 import b32encode
from hashlib import sha1
from StringIO import StringIO

environ = os.environ.copy()
setup_testing_defaults(environ)

class StartResponse(object):
    """Fake start response for our wsgi testing"""
    status = None
    headers = None
    def __call__(self, status, headers):
        assert self.status is None
        assert self.headers is None
        self.status = status
        self.headers = headers

class UploadTestCase(unittest.TestCase):
    """Test basic upload."""
    wsgiapp = UploadApp()
    upload_uid = 'FOOBARBAZ5'
    upload_size = (CHUNK_SIZE * 3) - 100

    def setUp(self):
        pass

    def request(self, path, data, method='POST', headers={}, expected=200):
        """Make a json request and return the result"""
        #request.add_header('Content-Type', 'application/json')
        response = StartResponse()
        environ['REQUEST_METHOD'] = method
        environ['PATH_INFO'] = '/%s' % path
        if isinstance(data, dict):
            environ['wsgi.input'] = StringIO(json.dumps(data))
        elif isinstance(data, basestring):
            environ['wsgi.input'] = data
        for key in headers.keys():
            envh = 'HTTP_%s' % key.upper().replace('-', '_')
            environ[envh] = headers[key]
        content = self.wsgiapp(environ, response)
        httpcode, message = response.status.split(' ', 1)
        if int(httpcode) != expected:
            raise ValueError("Expected HTTP %d and got %s" % (expected, response.status))
        return json.loads(content)

    def chunk_request(self, leaf_index, leaf_data, expected=201):
        """Make a chunk upload request and return data"""
        leaf_hash = b32encode(sha1(leaf_data).digest())
        # We save the hash for future testing use
        request_path = '%s/%d' % (self.upload_uid, leaf_index)
        return self.request(request_path, leaf_data, expected=expected, 
            headers={'x-dmedia-chash': leaf_hash}, method='PUT'), leaf_hash

    def test_01_upload_creation(self):
        """Upload Creation"""
        c = self.request('', {
            'quick_id': self.upload_uid,
            'bytes': self.upload_size,
        }, expected=202)
        self.assertEqual(c['quick_id'], self.upload_uid)
        self.assertEqual(c['bytes'], self.upload_size)
        self.assertEqual(c['leaf_size'], CHUNK_SIZE)
        self.assertEqual(len(c['leaves']), 3)
        self.assertEqual(c['leaves'][0], None)

    def test_02_upload_chunk(self):
        """Upload First Chunk"""
        # Make our request to save a chunk of data
        c, h = self.chunk_request(0, '^' * CHUNK_SIZE)
        # Test the full response from the chunk save
        self.assertEqual(c['quick_id'], self.upload_uid)
        self.assertEqual(c['success'], True)
        self.assertEqual(c['leaves'][0], h)
        r = c['received']
        self.assertEqual(r['index'], 0)
        self.assertEqual(r['size'], CHUNK_SIZE)
        self.assertEqual(r['chash'], h)

    def test_03_upload_resume(self):
        """Upload Resume"""
        c = self.request('', {
            'quick_id': self.upload_uid,
            'bytes': self.upload_size,
        }, expected=202)
        self.assertEqual(c['quick_id'], self.upload_uid)
        self.assertNotEqual(c['leaves'][0], None)

    def test_04_reupload_chunk(self):
        """Reupload Same Chunk"""
        pass # We don't know what this should do

    def test_05_small_chunk(self):
        """Upload a chunk too small"""
        c, h = self.chunk_request(2, '%%', expected=400)
        self.assertEqual(c['success'], False)

    def test_06_large_chunk(self):
        """Upload a chunk too large"""
        c, h = self.chunk_request(2, '%' * CHUNK_SIZE, expected=400)
        self.assertEqual(c['success'], False)

    def test_07_continue_upload(self):
        """Upload End Chunk"""
        leaf_size = CHUNK_SIZE + (self.upload_size - (CHUNK_SIZE * 3))
        c, h = self.chunk_request(2, '&' * leaf_size)
        self.assertEqual(c['success'], True)
        self.assertEqual(c['received']['size'], leaf_size)
        self.assertEqual(c['leaves'][2], h)

    def test_08_hash_mismatch(self):
        """Chunk Hash Error"""
        request_path = '%s/%d' % (self.upload_uid, 1)
        leaf_data = ' ' * CHUNK_SIZE
        leaf_hash = 'INCORRECT_HASH'
        c = self.request(request_path, leaf_data, method='PUT',
            headers={'x-dmedia-chash': leaf_hash}, expected=400)
        self.assertEqual(c['quick_id'], self.upload_uid)
        self.assertEqual(c['success'], False)

    def test_09_premature_finish(self):
        """Upload's Premature Finish"""
        c = self.request(self.upload_uid, {
            'chash': 'NONE',
            'bytes': self.upload_size,
            "name": 'MVI_5751.MOV',
            "mime": 'video/quicktime',
        }, expected=400)
        self.assertEqual(c['success'], False)

    def test_10_final_chunk(self):
        """Upload Last Chunk"""
        c, h = self.chunk_request(1, '*' * CHUNK_SIZE)
        self.assertEqual(c['success'], True)

    def test_11_finish_upload(self):
        """Upload Finish"""
        chash = 'UNKNOWN_HASH'
        name = "MVI_5751.MOV"
        mime = "video/quicktime"
        c = self.request(self.upload_uid, {
            'chash': chash,
            'bytes': self.upload_size,
            "name": name,
            "mime": mime,
        }, expected=201)

        self.assertEqual(c['success'], True)
        self.assertEqual(c['quick_id'], self.upload_uid)

    def test_12_file_exists(self):
        """Uploaded File Exists"""
        path = os.path.join(UPLOAD_DIR, 'files', self.upload_uid)
        self.assertTrue(os.path.exists(path))

    def test_13_replace_upload(self):
        """Upload Again"""
        pass # We don't know what to do yet
            

if __name__ == '__main__':
    test_support.run_unittest(
       UploadTestCase,
    )


