from os import path
import json
import logging

import gi
gi.require_version('WebKit2', '4.0')
from gi.repository import GLib, GObject, Gtk, WebKit2


log = logging.getLogger()
ui = path.join(path.dirname(path.abspath(__file__)), 'ui')
assert path.isdir(ui)


class Hub(GObject.GObject):
    def __init__(self, view):
        super().__init__()
        self._view = view
        view.connect('notify::title', self._on_notify_title)

    def _on_notify_title(self, view, notify):
        title = view.get_property('title')
        if title is None:
            return
        obj = json.loads(title)
        self.emit(obj['signal'], *obj['args'])

    def send(self, signal, *args):
        """
        Emit a signal by calling the JavaScript Signal.recv() function.
        """
        script = 'Hub.recv({!r})'.format(
            json.dumps({'signal': signal, 'args': args})
        )
        self._view.execute_script(script)
        self.emit(signal, *args)


def iter_gsignals(signals):
    assert isinstance(signals, dict)
    for (name, argnames) in signals.items():
        assert isinstance(argnames, list)
        args = [GObject.TYPE_PYOBJECT for argname in argnames]
        yield (name, (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE, args))


def hub_factory(signals):
    if signals:
        class FactoryHub(Hub):
            __gsignals__ = dict(iter_gsignals(signals))
        return FactoryHub
    return Hub


def wrap_in_scroll(child):
    scroll = Gtk.ScrolledWindow()
    scroll.set_policy(Gtk.PolicyType.AUTOMATIC, Gtk.PolicyType.AUTOMATIC)
    scroll.add(child)
    return scroll


class BaseUI:
    inspector = None
    signals = None
    title = 'Dmedia Setup'  # Default Gtk.Window title
    page = 'peering.html'  # Default page to load once CouchDB is available
    width = 960  # Default Gtk.Window width
    height = 540  # Default Gtk.Window height

    def __init__(self):
        self.build_window()
        self.window.connect('destroy', Gtk.main_quit)
        self.window.connect('delete-event', self.on_delete_event)
        self.hub = hub_factory(self.signals)(self.view)
        self.connect_hub_signals(self.hub)

    def connect_hub_signals(self, hub):
        pass

    def run(self):
        self.window.show_all()
        Gtk.main()

    def build_window(self):
        self.window = Gtk.Window()
        self.window.set_position(Gtk.WindowPosition.CENTER)
        self.window.set_default_size(self.width, self.height)
        self.window.set_title(self.title)
        self.vpaned = Gtk.VPaned()
        self.window.add(self.vpaned)
        self.view = WebKit2.WebView()
        self.view.get_settings().set_property('enable-developer-extras', True)
        inspector = self.view.get_inspector()
        #inspector.connect('inspect-web-view', self.on_inspect)
        self.view.load_uri('file://' + path.join(ui, self.page))
        scroll = wrap_in_scroll(self.view)
        self.vpaned.pack1(scroll, True, True)

    def on_inspect(self, *args):
        assert self.inspector is None
        self.inspector = WebKit2.WebView()
        pos = self.window.get_allocated_height() * 2 // 3
        self.vpaned.set_position(pos)
        self.vpaned.pack2(self.inspector, True, True)
        self.inspector.show_all()
        return self.inspector


class ServerUI(BaseUI):
    page = 'server.html'

    signals = {
        'get_secret': [],
        'display_secret': ['secret', 'typo'],
    }

    def __init__(self, Dmedia, peer_id):
        super().__init__()
        self.Dmedia = Dmedia
        self.peer_id = peer_id
        Dmedia.connect_to_signal('DisplaySecret', self.on_DisplaySecret)
        Dmedia.connect_to_signal('PeeringDone', self.on_PeeringDone)

    def connect_hub_signals(self, hub):
        hub.connect('get_secret', self.on_get_secret)

    def on_delete_event(self, *args):
        self.Dmedia.Cancel(self.peer_id)

    def on_get_secret(self, hub):
        self.Dmedia.GetSecret(self.peer_id)

    def on_DisplaySecret(self, secret, typo): 
        GLib.idle_add(self.hub.send, 'display_secret', secret, typo)

    def on_PeeringDone(self): 
        self.window.destroy()


class ClientUI(BaseUI):
    page = 'client.html'

    signals = {
        'create_user': [],
        'peer_with_existing': [],

        'accept': [],
        'have_secret': ['secret'],
        'response': ['success'],

        'user_ready': [],
        'message': ['message'],

    }

    def __init__(self, Dmedia):
        super().__init__()
        self.Dmedia = Dmedia
        self.quit = False
        #Dmedia.connect_to_signal('Message', self.on_Message)
        Dmedia.connect_to_signal('Accept', self.on_Accept)
        Dmedia.connect_to_signal('Response', self.on_Response)
        Dmedia.connect_to_signal('InitDone', self.on_InitDone)
        self.done = set()

    def on_delete_event(self, *args):
        self.quit = True

    def connect_hub_signals(self, hub):
        hub.connect('create_user', self.on_create_user)
        hub.connect('peer_with_existing', self.on_peer_with_existing)
        hub.connect('have_secret', self.on_have_secret)
        hub.connect('user_ready', self.on_user_ready)

    def on_Message(self, message):
        self.hub.send('message', message)

    def on_Accept(self):
        GLib.idle_add(self.hub.send, 'accept')

    def on_Response(self, success):
        GLib.idle_add(self.hub.send, 'response', success)

    def on_InitDone(self):
        self.done.add('init_done')
        self.do_destroy()

    def on_user_ready(self, hub):
        self.done.add('user_ready')
        self.do_destroy()

    def do_destroy(self):
        if self.done == set(['init_done', 'user_ready']):
            self.window.destroy()

    def on_create_user(self, hub):
        self.Dmedia.CreateUser()

    def on_peer_with_existing(self, hub):
        self.Dmedia.PeerWithExisting()

    def on_have_secret(self, hub, secret):
        self.Dmedia.SetSecret(secret)
