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
Download with BitTorrent.
"""

import logging
import time
from os import path

import libtorrent

from dmedia.constants import LEAF_SIZE
from dmedia.transfers import HTTPBaseBackend, register_downloader, http_conn


log = logging.getLogger()


class TorrentBackend(HTTPBaseBackend):
    """
    Backend for BitTorrent downloads using `libtorrent`.
    """

    def download(self, doc, leaves, fs):
        chash = doc['_id']
        ext = doc.get('ext')
        url = self.basepath + self.key(chash, ext) + '?torrent'
        data = self.get(url)

        tmp = fs.tmp(chash, ext, create=True)
        session = libtorrent.session()
        session.listen_on(6881, 6891)

        info = libtorrent.torrent_info(libtorrent.bdecode(data))

        torrent = session.add_torrent({
            'ti': info,
            'save_path': path.dirname(tmp),
        })

        while not torrent.is_seed():
            s = torrent.status()
            self.progress(s.total_payload_download)
            time.sleep(2)

        session.remove_torrent(torrent)
        time.sleep(2)


register_downloader('torrent', TorrentBackend)
