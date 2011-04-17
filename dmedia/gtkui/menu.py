# Authors:
#   David Green <david4dev@gmail.com>
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

#TODO: separators, keyboard acceleration

from gi.repository import Gtk

ACTIONS = {
    "close" : Gtk.main_quit
}


#this could potentially be put into a .json file
MENU = [
    {
        "label" : "_File",
        "type" : "menu",
        "items" : [
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            }
        ]
    }
]

#A menu for testing and demonstration
TEST_MENU = [
{
        "label" : "_File",
        "type" : "menu",
        "items" : [
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "type" : "custom",
                "widget" : Gtk.SeparatorMenuItem()
            },
            {
                "label" : "_Close",
                "type" : "menu",
                "items" : [{
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "type" : "custom",
                "widget" : Gtk.SeparatorMenuItem()
            },
            {
                "label" : "_Close",
                "type" : "menu",
                "items" : [{
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "type" : "custom",
                "widget" : Gtk.SeparatorMenuItem()
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            }]
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            }]
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            }
        ]
    },
    {
        "label" : "_Edit",
        "type" : "menu",
        "items" : [
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "type" : "custom",
                "widget" : Gtk.SeparatorMenuItem()
            },
            {
                "label" : "_Close",
                "type" : "menu",
                "items" : [{
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "type" : "custom",
                "widget" : Gtk.SeparatorMenuItem()
            },
            {
                "label" : "_Close",
                "type" : "menu",
                "items" : [{
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "type" : "custom",
                "widget" : Gtk.SeparatorMenuItem()
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            }]
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            }]
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            }
        ]
    },
    {
        "label" : "_View",
        "type" : "menu",
        "items" : [
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "type" : "custom",
                "widget" : Gtk.SeparatorMenuItem()
            },
            {
                "label" : "_Close",
                "type" : "menu",
                "items" : [{
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "type" : "custom",
                "widget" : Gtk.SeparatorMenuItem()
            },
            {
                "label" : "_Close",
                "type" : "menu",
                "items" : [{
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "type" : "custom",
                "widget" : Gtk.SeparatorMenuItem()
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            }]
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            }]
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            }
        ]
    },
    {
        "label" : "_Tools",
        "type" : "menu",
        "items" : [
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "type" : "custom",
                "widget" : Gtk.SeparatorMenuItem()
            },
            {
                "label" : "_Close",
                "type" : "menu",
                "items" : [{
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "type" : "custom",
                "widget" : Gtk.SeparatorMenuItem()
            },
            {
                "label" : "_Close",
                "type" : "menu",
                "items" : [{
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "type" : "custom",
                "widget" : Gtk.SeparatorMenuItem()
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            }]
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            }]
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            }
        ]
    },
    {
        "label" : "Et_c",
        "type" : "menu",
        "items" : [
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "type" : "custom",
                "widget" : Gtk.SeparatorMenuItem()
            },
            {
                "label" : "_Close",
                "type" : "menu",
                "items" : [{
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "type" : "custom",
                "widget" : Gtk.SeparatorMenuItem()
            },
            {
                "label" : "_Close",
                "type" : "menu",
                "items" : [{
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            },
            {
                "type" : "custom",
                "widget" : Gtk.SeparatorMenuItem()
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            }]
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            }]
            },
            {
                "label" : "_Close",
                "type" : "action",
                "action" : "close"
            }
        ]
    }
]
