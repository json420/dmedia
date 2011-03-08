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
Simple WSGI server for uploading content using json.
"""

import json
import logging
import os
from base64 import b32encode
from hashlib import sha1
from wsgiref import simple_server

HTTP_PORT = 9500
CHUNK_SIZE = 205000
UPLOAD_DIR = '/tmp/uploads/'

class UploadExistError(IOError):
    pass

class UploadDataError(IOError):
    pass

class Upload(object):
    """Reprisent an upload"""
    def __init__(self, upload_dir, uid, size=None):
        self.dirs = [ 'active', 'complete', 'cache', 'files' ]
        self.upload_dir = upload_dir

        # Make sure all of our target directories are created.
        for dtype in self.dirs:
            directory = os.path.join(upload_dir, dtype)
            if not os.path.exists(directory):
                os.makedirs(directory)

        # Are we resuming or creating a new upload
        self.uid = uid
        if not size:
            # We could append a resume datetime for logs
            self.datum = self.load_datum()
        else:
            # We could add a start datetime for logs
            self.datum = { 'size': size }
            self.save_datum()

    def get_path(self, ptype, ext=None):
        """Return a valid path for this file"""
        filename = ext and self.uid + '.' + ext or self.uid
        return os.path.join(self.upload_dir, self.dirs[ptype], filename)

    @property
    def _ufn(self): # Upload meta data filename
        return self.get_path(0, 'upload')

    @property
    def _cfn(self): # Complete meta data filename
        return self.get_path(1, 'upload')

    @property
    def _afn(self): # Active upload data filename
        return self.get_path(2)

    @property
    def _ffn(self): # Finished uploaded data filename
        return self.get_path(3)

    def send(self, status):
        """Send a basic response with all data"""
        return {
            'status'    : status,
            'quick_id'  : self.uid,
            "bytes"     : self.size,
            "leaf_size" : self.leaf_size,
            "leaves"    : self.leaves,
        }

    def load_datum(self):
        """Load the meta-data file for a given upload"""
        try:
            with open(self._ufn) as fhl:
                return json.loads(fhl.read())
        except IOError:
            raise UploadExistError("Can't find upload from %s" % self._ufn)
        except ValueError:
            raise UploadExistError("Can't load upload saved %s" % self._ufn)

    def save_datum(self):
        """Save the meta-data back to file for this upload"""
        try:
            with open(self._ufn, 'w') as fhl:
                fhl.write(json.dumps(self.datum))
        except IOError:
            raise UploadDataError("Can't save upload data to %s" % self._ufn)

    def leaf_hash(self, stream):
        """Return the hash and size of the stream"""
        lhash = b32encode(sha1(stream).digest())
        return lhash, len(stream) # This is not implimented XXX

    def mark_leaf(self, index, pot):
        """Mark a leaf as done and save meta data"""
        self.leaves[index] = pot
        # We might want to save a start/end datetime stamp
        # in future for logs of uploads and performance.
        self.save_datum()

    def save(self, index, sent_hash, stream):
        """Save a chunk into place and report the checksum"""
        # the last chunk might not be the right size
        leaf_size = CHUNK_SIZE
        if index == len(self.leaves)-1:
            leaf_size = self.size - (CHUNK_SIZE * (len(self.leaves)-1))
        success = False
        failure = None
        calc_hash, stream_size = self.leaf_hash(stream)
        if calc_hash == sent_hash:
            if stream_size == leaf_size:
                success = True
                try:
                    fhl = open(self._afn, 'r+b')
                except IOError:
                    fhl = open(self._afn, 'w')
                fhl.seek(index * CHUNK_SIZE)
                fhl.write(stream)
                fhl.close()
                self.mark_leaf(index, sent_hash)
            else:
                failure = 'Bad Chunk Size (%d->%d)' % (stream_size, leaf_size)
        else:
            failure = 'Bad Hash (%s->%s)' % (sent_hash, calc_hash)
        return {
            "index": index,
            "chash": calc_hash,
            "size": stream_size
        }, success, failure

    def finish(self, extra_data):
        """Finish off anything"""
        if None in self.leaves:
            return False
        # Save extra data to datum before it gets moved
        self.datum['extra'] = extra_data
        self.save_datum()
        # Move meta data file for logs and for data access
        os.rename(self._ufn, self._cfn)
        os.rename(self._afn, self._ffn)
        # check total hash XXX
        return True

    @property
    def leaf_size(self):
        """return how big the leafs will be"""
        if not self.datum.has_key('leaf_size'):
            self.datum['leaf_size'] = CHUNK_SIZE
        return self.datum['leaf_size']

    @property
    def size(self):
        """Returns a total file size of the upload"""
        return self.datum['size']

    @property
    def leaves(self):
        """Return a list of leaf nodes"""
        if not self.datum.has_key('leaves'):
            leaf_count = int(self.size / self.leaf_size) + 1
            self.datum['leaves'] = [None] * leaf_count
        return self.datum['leaves']


class UploadApp(object):
    """Control requests from the client"""
    def __init__(self, upload_dir=UPLOAD_DIR):
        self.upload_dir = upload_dir

    def __call__(self, env, respond):
        """The upload App has three basic modes, start, upload a chunk
        and finish, see the documentation about how to use each."""
        path = env['PATH_INFO'][1:]
        method = env['REQUEST_METHOD']
        data = env['wsgi.input']
        try:
            if method == 'GET':
                raise KeyError("Invalid Request")
            elif method == 'PUT':
                (uid, index) = path.split('/')[:2]
                chash = env.get('HTTP_X_DMEDIA_CHASH', None)
                response = self.upload_chunk(uid, int(index), chash, data)
            elif path == '':
                response = self.start_upload(self.json(data))
            else:
                response = self.finish_upload(path, self.json(data))
        except Exception, e:
            response = {
                'status' : '500 %s' % str(e),
                'success' : False,
                'error' : True,
            }
            raise
        respond(response.pop('status'), [('Content-type', 'application/json')])
        return json.dumps(response)

    def json(self, io):
        """Return a structure based on a json input"""
        string = io.readline(1024)
        try: return json.loads(string)
        except TypeError:
            print "Couldn't load '%s' as json data" % string
        return {}

    def get_upload(self, uid, size=None):
        """Return an existing upload"""
        try:
            return Upload(self.upload_dir, uid, size)
        except UploadExistError:
            return None

    def upload_chunk(self, uid, leaf, chash, stream):
        """Upload a chunk of data"""
        upload = self.get_upload(uid)
        if upload:
            received, success, failure = upload.save(leaf, chash, stream)
            if success:
                result = upload.send('201 Created')
                result['success'] = True
            else:
                result = upload.send('400 Chunk Rejected - %s' % failure)
                result['success'] = False
            result['received'] = received
            return result
        raise ValueError # 404 really

    def start_upload(self, jdatum):
        """Start a fresh upload, or continue an upload"""
        uid = jdatum['quick_id']
        upload = self.get_upload(uid)
        if not upload:
            logging.debug("Starting Upload '%s'" % uid)
            upload = self.get_upload(uid, jdatum['bytes'])
        else:
            logging.debug("Resuming Upload '%s'" % uid)
        return upload.send('202 Accepted')

    def finish_upload(self, uid, jdatum):
        """End an upload, all chunks recieved and checked"""
        upload = self.get_upload(uid)
        if upload:
            if upload.finish(jdatum):
                return {
                    'status'   : '201 Finished',
                    'success'  : True,
                    'quick_id' : uid,
                }
            return { 'status' : '400 Pieces Missing', 'success' : False }
        raise ValueError

if __name__ == '__main__':
    httpd = simple_server.make_server('', HTTP_PORT, UploadApp())
    print "Upload HTTP App served on port %d..." % HTTP_PORT
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print "Gracefully Finished!"

