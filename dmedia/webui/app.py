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
DMedia browser app.
"""

from gettext import gettext as _

from . import base


class Browser(base.Page):
    title = 'DMedia Browser'
    body = ('browser.xml', None)

    css = (
        ('style.css', None),
    )

    js = (
        ('couch.js', None),
        ('browser.js', None),
    )

    assets = (
        ('search.png', None, None),
        ('stars.png', None, None),
    )

    def get_body_vars(self):
        return {
            'meta': [
                dict(name='name', text='File Name:'),
                dict(name='fps', text='FPS:'),
                dict(name='aperture', text='Aperture:'),
            ],
        }

class App(base.App):
    pages = [Browser]
