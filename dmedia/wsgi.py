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


Start/Resume an Import
----------------------

Whether you're starting or resuming an import, the request is the same.  The
request will look like this:

    ::

        POST /import HTTP/1.1
        Content-Type: application/json

        {
            "quick_id": "GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN",
            "size": 20202333
        }


The response will contain a list with a spot for each leaf (chunk) in the
import.  For each position in the list, the value with either be the
content-hash of that leaf (meaning the leaf has been successfully imported) or
null (meaning the leaf was never imported or was not imported successfully).

For example, if starting a new import, the response would look like this:

    ::

        HTTP/1.1 202 Accepted
        Content-Type: application/json

        {
            "quick_id": "GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN",
            "size": 20202333,
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
            "size": 20202333,
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

        PUT /import/GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN/0 HTTP/1.1
        Content-Type: application/octet-stream
        Content-Length: 8388608
        x-dmedia-chash: IXJTSUCYYFECGSG6JIB2R77CAJVJK4W3

        <LEAF DATA>


The *x-dmedia-chash* header above is the base32-encoded sha1 hash of the leaf
content.  If the server computes the same content-hash for the leaf on the
receiving end (meaning no data errors occurred), the response would look like
this:

    ::

        HTTP/1.1 201 Created
        Content-Type: application/json

        {
            "success": true,
            "received": {
                "index": 0,
                "chash": "IXJTSUCYYFECGSG6JIB2R77CAJVJK4W3",
                "size": 8388608
            },
            "quick_id": "GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN",
            "size": 20202333,
            "leaf_size" 8388608,
            "leaves" [
                "IXJTSUCYYFECGSG6JIB2R77CAJVJK4W3",
                "MA3IAHUOKXR4TRG7CWAPOO7U4WCV5WJ4",
                null
            ]
        }


If the content-hash did not match on the receiving end, the response would look
like this:

    ::

        HTTP/1.1 400 Bad Request
        Content-Type: application/json

        {
            "success": false,
            "received": {
                "index": 0,
                "chash": "F6ATTKI6YVWVRBQQESAZ4DSUXQ4G457A",
                "size": 27328
            },
            "expected": {
                "chash": "IXJTSUCYYFECGSG6JIB2R77CAJVJK4W3",
                "size": 8388608
            },
            "quick_id": "GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN",
            "size": 20202333,
            "leaf_size" 8388608,
            "leaves" [
                null,
                "MA3IAHUOKXR4TRG7CWAPOO7U4WCV5WJ4",
                null
            ]
        }


Once the 1st leaf has been successfully imported, you would import the 3rd leaf
with a request like this:

    ::

        PUT /import/GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN/2 HTTP/1.1
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
            "success": true,
            "received": {
                "index": 0,
                "chash": "FHF7KDMAGNYOVNYSYT6ZYWQLUOCTUADI",
                "size": 3425117
            },
            "quick_id": "GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN",
            "size": 20202333,
            "leaf_size" 8388608,
            "leaves" [
                "IXJTSUCYYFECGSG6JIB2R77CAJVJK4W3",
                "MA3IAHUOKXR4TRG7CWAPOO7U4WCV5WJ4",
                "FHF7KDMAGNYOVNYSYT6ZYWQLUOCTUADI"
            ],
            "chash": "ZR765XWSF6S7JQHLUI4GCG5BHGPE252O"
        }


Notice that now that all the leaves have been imported, the response JSON has
the ``"chash"`` key... the top-hash, or overall content-hash of the file.  At
this point you complete the import.


Finish the Import
-----------------

To finish the import, just post back the response from the final leaf import to
``"/imports/QUICK_ID"``.  For the above import, the request would look like
this:

        POST /import HTTP/1.1
        Content-Type: application/json

        {
            "success": true,
            "received": {
                "index": 0,
                "chash": "FHF7KDMAGNYOVNYSYT6ZYWQLUOCTUADI",
                "size": 3425117
            },
            "quick_id": "GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN",
            "size": 20202333,
            "leaf_size" 8388608,
            "leaves" [
                "IXJTSUCYYFECGSG6JIB2R77CAJVJK4W3",
                "MA3IAHUOKXR4TRG7CWAPOO7U4WCV5WJ4",
                "FHF7KDMAGNYOVNYSYT6ZYWQLUOCTUADI"
            ],
            "chash": "ZR765XWSF6S7JQHLUI4GCG5BHGPE252O"
        }

"""
