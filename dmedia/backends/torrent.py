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

from dmedia.constants import LEAF_SIZE
from dmedia import transfers


log = logging.getLogger()


class TorrentBackend(transfers.TransferBackend):
    """
    Backend for BitTorrent downloads using `libtorrent`.
    """

    def get_tmp(self):
        tmp = self.fs.tmp(self.chash, self.ext, create=True)
        log.debug('Writting file to %r', tmp)
        return tmp

    def finalize(self):
        dst = self.fs.tmp_verify_move(self.chash, self.ext)
        log.debug('Canonical name is %r', dst)
        return dst

    def run(self):
        log.info('Downloading torrent %r %r', self.chash, self.ext)
        tmp = self.get_tmp()
        session = libtorrent.session()
        session.listen_on(6881, 6891)

        info = libtorrent.torrent_info(
            libtorrent.bdecode(self.torrent)
        )

        torrent = session.add_torrent({
            'ti': info,
            'save_path': path.dirname(tmp),
        })

        while not torrent.is_seed():
            s = torrent.status()
            log.debug('Downloaded %d%%', s.progress * 100)
            time.sleep(2)

        session.remove_torrent(torrent)
        time.sleep(1)

        return self.finalize()


transfers.register_downloader('torrent', TorrentBackend)
