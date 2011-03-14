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
WSGI HTTP file transfer server (upload and download).


Import REST API
===============

The import REST API is designed to allow files to be imported into dmedia from
any browser that supports the HTML5 File API.  The process is very similar to
how local imports work.  When the import starts, the file content-hash isn't
yet known, but is computed as the file is imported.

Unlike local imports, the browser must compute the content-hash of each leaf
before uploading, and the server verifies the integrity of the leaf on the
receiving end.  Also unlike local imports, the HTTP import can be resumed (in
case of lost connectivity), and the leaves can be uploaded in any order, or even
in parallel.

The API is presented relative to the mount point of the import app.  Depending
on where the app is mounted, all the resources might start with, say,
``"/imports/"`` instead of ``"/"``.


Start/Resume an Import
----------------------

Whether you're starting or resuming an import, the request is the same.  The
request will look like this:

    ::

        POST / HTTP/1.1
        Content-Type: application/json

        {
            "quick_id": "GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN",
            "bytes": 20202333
        }


The response will contain a list with a spot for each leaf (chunk) in the
import.  For each position in the list, the value with either be the
content-hash of that leaf (meaning the leaf has been successfully imported) or
null (meaning the leaf was never imported or was not imported successfully).

For example, if starting a new import, the response would look like this:

    ::

        HTTP/1.1 201 Created
        Content-Type: application/json

        {
            "quick_id": "GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN",
            "bytes": 20202333,
            "leaf_size" 8388608,
            "leaves" [
                null,
                null,
                null
            ]
        }


If resuming an import where you had previously imported the 2nd leaf, the
response would look like this:

    ::

        HTTP/1.1 202 Accepted
        Content-Type: application/json

        {
            "quick_id": "GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN",
            "bytes": 20202333,
            "leaf_size" 8388608,
            "leaves" [
                null,
                "MA3IAHUOKXR4TRG7CWAPOO7U4WCV5WJ4",
                null
            ]
        }


Import a Leaf
-------------

Say you resume an import and receive the above response: the 2nd leaf has been
imported, the 1st and 3rd leaves still need to be imported.  You would import
the 1st leaf with a request like this:

    ::

        PUT /GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN/0 HTTP/1.1
        Content-Type: application/octet-stream
        Content-Length: 8388608
        x-dmedia-chash: IXJTSUCYYFECGSG6JIB2R77CAJVJK4W3

        <LEAF DATA>


The *x-dmedia-chash* header above is the base32-encoded sha1 hash of the leaf
content.  If the server computes the same content-hash for the leaf on the
receiving end (meaning no data corruption occurred), the response would look
like this:

    ::

        HTTP/1.1 201 Created
        Content-Type: application/json

        {
            "quick_id": "GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN",
            "index": 0,
            "received": "IXJTSUCYYFECGSG6JIB2R77CAJVJK4W3",
        }


If the content-hash did not match on the receiving end, the response would look
like this:

    ::

        HTTP/1.1 412 Precondition Failed
        Content-Type: application/json

        {
            "quick_id": "GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN",
            "index": 0,
            "received": "F6ATTKI6YVWVRBQQESAZ4DSUXQ4G457A",
            "expected": "IXJTSUCYYFECGSG6JIB2R77CAJVJK4W3",
        }


If you try to upload the leaf of a multi-part upload that does not exist, the
response would look like this:


        HTTP/1.1 409 Conflict
        Content-Type: application/json

        {"quick_id": "GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN"}


Once the 1st leaf has been successfully imported, you would import the 3rd leaf
with a request like this:

    ::

        PUT /GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN/2 HTTP/1.1
        Content-Type: application/octet-stream
        Content-Length: 3425117
        x-dmedia-chash: FHF7KDMAGNYOVNYSYT6ZYWQLUOCTUADI

        <LEAF DATA>


Assuming the content-hash checks out on the receiving end, you would get a
response like this:

    ::

        HTTP/1.1 201 Created
        Content-Type: application/json

        {
            "quick_id": "GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN",
            "index": 0,
            "received": "FHF7KDMAGNYOVNYSYT6ZYWQLUOCTUADI",
        }


Now that all the leaves have uploaded, you can finish the import.


Finish the Import
-----------------

Finally, you finish the import with a request like this:

    ::

        POST /GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN HTTP/1.1
        Content-Type: application/json

        {
            "bytes": 20202333,
            "name": "MVI_5751.MOV",
            "mime": "video/quicktime",
            "leaves" [
                "IXJTSUCYYFECGSG6JIB2R77CAJVJK4W3",
                "MA3IAHUOKXR4TRG7CWAPOO7U4WCV5WJ4",
                "FHF7KDMAGNYOVNYSYT6ZYWQLUOCTUADI"
            ]
        }


If everything went well on the server (all leaves were actually present and had
correct content-hash), the response will contain the CouchDB document
corresponding to this newly imported file:

    ::

        HTTP/1.1 201 Created
        Content-Type: application/json

        {
            "success": true,
            "doc": {
                "_id": "ZR765XWSF6S7JQHLUI4GCG5BHGPE252O",
                "quick_id": "GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN",
                "type": "dmedia/file",
                "time": 1234567890,
                "bytes": 20202333,
                "ext" "mov",
                "origin": "user",
                "name": "MVI_5751.MOV",
                "mime": "video/quicktime",
                "stored": {
                    "MZZG2ZDSOQVSW2TEMVZG643F": {
                        "copies": 2,
                        "time": 123456789
                    }
                }
            }
        }


If you try to finish a multi-part upload that does not exist, the response would
look like this:


        HTTP/1.1 409 Conflict
        Content-Type: application/json

        {"quick_id": "GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN"}

"""
