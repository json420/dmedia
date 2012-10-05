from os import path
import json

from gi.repository import GObject, Gtk, WebKit


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


class BaseUI:
    inspector = None
    signals = None
    title = 'Novacut'  # Default Gtk.Window title
    page = 'peering.html'  # Default page to load once CouchDB is available
    width = 960  # Default Gtk.Window width
    height = 540  # Default Gtk.Window height

    def __init__(self):
        self.build_window()
        self.hub = hub_factory(self.signals)(self.view)
        self.connect_hub_signals(self.hub)

    def run(self):
        self.window.show_all()
        Gtk.main()

    def connect_hub_signals(self, hub):
        pass

    def build_window(self):
        self.window = Gtk.Window()
        self.window.connect('destroy', self.quit)
        self.window.set_position(Gtk.WindowPosition.CENTER)
        self.window.set_default_size(self.width, self.height)
        self.window.set_title(self.title)
        self.vpaned = Gtk.VPaned()
        self.window.add(self.vpaned)
        self.view = WebKit.WebView()
        self.view.get_settings().set_property('enable-developer-extras', True)
        inspector = self.view.get_inspector()
        inspector.connect('inspect-web-view', self.on_inspect)
        self.view.load_uri('file://' + path.join(ui, self.page))
        self.vpaned.pack1(self.view, True, True)

    def quit(self, *arg):
        Gtk.main_quit()

    def on_inspect(self, *args):
        assert self.inspector is None
        self.inspector = WebKit.WebView()
        pos = self.window.get_allocated_height() * 2 // 3
        self.vpaned.set_position(pos)
        self.vpaned.pack2(self.inspector, True, True)
        self.inspector.show_all()
        return self.inspector

