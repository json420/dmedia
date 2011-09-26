# userwebkit: so WebKitGtk apps can to talk to a usercouch
# Copyright (C) 2011 Novacut Inc
# 
# This file is part of `userwebkit`.
# 
# `userwebkit` is free software: you can redistribute it and/or modify it under
# the terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
# 
# `userwebkit` is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
# details.
# 
# You should have received a copy of the GNU Lesser General Public License along
# with `userwebkit`.  If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>

"""
`userwebkit` - so WebKitGtk apps can to talk to a `usercouch`.
"""

import sys
from os import path
from urllib.parse import urlparse, parse_qsl
import json

import microfiber
from microfiber import _oauth_header, _basic_auth_header
from gi.repository import GObject, Gtk, WebKit, Gio


GObject.threads_init()

__version__ = '11.09.0'
APPS = '/usr/share/couchdb/apps/'


class DBus:
    def __init__(self, conn):
        self.conn = conn

    def get(self, bus, path, iface=None):
        if iface is None:
            iface = bus
        return Gio.DBusProxy.new_sync(
            self.conn, 0, None, bus, path, iface, None
        )

    def get_async(self, callback, bus, path, iface=None):
        if iface is None:
            iface = bus
        Gio.DBusProxy.new(
            self.conn, 0, None, bus, path, iface, None, callback, None
        )


session = DBus(Gio.bus_get_sync(Gio.BusType.SESSION, None))


def handler(d):
    assert path.abspath(d) == d
    return 	'{{couch_httpd_misc_handlers, handle_utils_dir_req, {}}}'.format(
        json.dumps(d)
    )


class CouchView(WebKit.WebView):
    def __init__(self, env=None):
        super().__init__()
        self.connect('resource-request-starting', self._on_request)
        self.set_env(env)

    def set_env(self, env):
        self._env = env
        if env is None:
            self._u = None
            self._oauth = None
            self._basic = None
            return
        self._u = urlparse(env['url'])
        self._oauth = env.get('oauth')
        self._basic = env.get('basic')

    def _on_request(self, view, frame, resource, request, response):
        if self._env is None:
            return
        uri = request.get_uri()
        u = urlparse(uri)
        if u.netloc != self._u.netloc:
            return
        if u.scheme != self._u.scheme:
            return
        if self._oauth:
            query = dict(parse_qsl(u.query))
            if u.query and not query:
                query = {u.query: ''}
            baseurl = ''.join([u.scheme, '://', u.netloc, u.path])
            method = request.props.message.props.method
            h = _oauth_header(self._oauth, method, baseurl, query)
        elif self._basic:
            h = _basic_auth_header(self._basic)
        else:
            return
        for (key, value) in h.items():
            request.props.message.props.request_headers.append(key, value)


class Inspector(Gtk.VBox):
    def __init__(self, env):
        super().__init__()

        hbox = Gtk.HBox()
        self.pack_start(hbox, False, False, 0)

        close = Gtk.Button(stock=Gtk.STOCK_CLOSE)
        hbox.pack_start(close, False, False, 0)
        close.connect('clicked', self.on_close)

        self.reload = Gtk.Button('Reload')
        hbox.pack_start(self.reload, False, False, 4)

        scroll = Gtk.ScrolledWindow()
        self.pack_start(scroll, True, True, 0)
        scroll.set_policy(Gtk.PolicyType.AUTOMATIC, Gtk.PolicyType.AUTOMATIC)

        self.view = CouchView(env)
        scroll.add(self.view)

    def on_close(self, button):
        self.destroy()


class BaseUI(object):
    app = 'foo'
    page = 'index.html'
    splash = 'splash.html'
    title = 'Fix Me'
    databases = tuple()

    proxy_bus = 'org.freedesktop.DC3'
    proxy_path = '/'

    def __init__(self, benchmark=False):
        self.benchmark = benchmark
        self.env = None

        # Figure out if we're running in-tree or not        
        script = path.abspath(sys.argv[0])
        tree = path.dirname(script)
        setup = path.join(tree, 'setup.py')
        ui = path.join(tree, 'ui')
        self.intree = (path.isfile(setup) and path.isdir(ui))
        self.ui = (ui if self.intree else path.join(APPS, self.app))

    def run(self):
        self.inspector = None
        self.window = Gtk.Window()
        self.window.connect('destroy', self.quit)
        self.window.maximize()
        self.window.set_default_size(960, 540)
        self.window.set_title(self.title)
        self.vpaned = Gtk.VPaned()
        self.window.add(self.vpaned)
        self.scroll = Gtk.ScrolledWindow()
        self.vpaned.pack1(self.scroll, True, True)
        self.scroll.set_policy(
            Gtk.PolicyType.AUTOMATIC, Gtk.PolicyType.AUTOMATIC
        )
        self.view = CouchView()
        self.scroll.add(self.view)
        self.view.get_settings().set_property('enable-developer-extras', True)
        inspector = self.view.get_inspector()
        inspector.connect('inspect-web-view', self.on_inspect)
        splash = open(path.join(self.ui, self.splash), 'r').read()
        self.view.load_string(splash, 'text/html', 'UTF-8', 'file:///')
        self.window.show_all()
        GObject.idle_add(self.on_idle)
        Gtk.main()

    def quit(self, *arg):
        # FIXME: This is a work-around for the segfault we're getting when
        # The window is destroyed before the inspector is
        if self.inspector:
            self.inspector.destroy()
        Gtk.main_quit()

    def on_idle(self):
        if self.benchmark:
            Gtk.main_quit()
            return
        session.get_async(self.on_proxy, self.proxy_bus, self.proxy_path)

    def on_proxy(self, proxy, async_result, *args):
        self.proxy = proxy
        env = json.loads(self.proxy.GetEnv())
        self.set_env(env)

    def set_env(self, env):    
        self.env = env
        self.server = microfiber.Server(env)    
        for name in self.databases:
            try:
                self.server.put(None, name)
            except microfiber.PreconditionFailed:
                pass
        if self.intree:
            url = '/_intree/' + self.page
            self.server.put(
                handler(self.ui), '_config', 'httpd_global_handlers', '_intree'
            )
        else:
            url = '/'.join(['/_apps', self.app, self.page])
        self.view.set_env(env)
        self.view.load_uri(self.server._full_url(url))

    def on_inspect(self, *args):
        self.inspector = Inspector(self.env)
        pos = self.window.get_allocated_height() * 2 // 3
        self.vpaned.set_position(pos)
        self.vpaned.pack2(self.inspector, True, True)
        self.inspector.show_all()
        self.inspector.reload.connect('clicked', self.on_reload)
        return self.inspector.view

    def on_reload(self, button):
        self.view.reload_bypass_cache()

