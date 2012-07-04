# dmedia: distributed media library
# Copyright (C) 2012 Novacut Inc
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
Distribute compute intensive workloads across multiple nodes.

We want to easily harness everything from a laptop to a local cluster to the
cloud get work done for the user.  We're talking about compute-intensive tasks
like rendering, transcoding, etc.

As these jobs will be using one or more files stored in Dmedia as input, it
makes sense to tightly couple the job scheduling with Dmedia.  For a node to
take on a job, it either needs to already have the needed files, or be able to
get them.

Worker Scripts
==============

Worker scripts have a simple JSON-in, JSON-out API agreement.  The script are
passed a JSON description of the job via standard input, and return a JSON
description of the result via standard output.

Dmedia ignores the details of both the job and the result.  Dmedia only cares
about which worker script and which files are needed to execute a job.  For
example, a dmedia/job document in CouchDB would look something like this:

>>> doc = {
...     '_id': 'H6VVCPDJZ7CSFG4V6EEYCPPD',
...     'ver': 0,
...     'type': 'dmedia/job',
...     'time': 1234567890,
...     'status': 'waiting',
...     'worker': 'novacut-renderer',
...     'files': [
...         'ROHNRBKS6T4YETP5JHEGQ3OLSBDBWRCKR2BKILJOA3CP7QZW',
...     ],
...     'job': {
...         'Dmedia': 'ignores everything in job',
...     },
... }


"""


def get_worker_script(name):
    return    


class TaskMaster:
    def run_job(self, doc):
        doc['time_start'] = time.time()
        doc['status'] = 'executing'
        doc['machine_id'] = self.machine_id
        try:
            self.db.save(doc)
        except Conflict:
            return False
        script = get_worker_script(doc['worker'])
        obj_s = json.dumps({'job': doc['job'], 'files': doc['files']})
        try:
            result = check_output([script, obj_s])
            doc['result'] = json.loads(result.decode('utf-8'))
            doc['status'] = 'completed'
        except CalledProcessError:
            doc['status'] = 'failed'
        doc['time_end'] = time.time()
        self.db.save(doc)
        return True

