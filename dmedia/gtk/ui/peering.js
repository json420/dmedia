"use strict";

var Hub = {
    /*
    Relay signals between JavaScript and Gtk.

    For example, to send a signal to Gtk via document.title:

    >>> Hub.send('click');
    >>> Hub.send('changed', 'foo', 'bar');

    Or from the Gtk side, send a signal to JavaScript by using
    WebView.execute_script() to call Hub.recv() like this:

    >>> Hub.recv('{"signal": "error", "args": ["oops!"]}');

    Use userwebkit.BaseApp.send() as a shortcut to do the above.

    Lastly, to emit a signal from JavaScript to JavaScript handlers, use
    Hub.emit() like this:

    >>> Hub.emit('changed', 'foo', 'bar');

    */
    i: 0,

    names: {},

    connect: function(signal, callback, self) {
        /*
        Connect a signal handler.

        For example:

        >>> Hub.connect('changed', this.on_changed, this);

        */
        if (! Hub.names[signal]) {
            Hub.names[signal] = [];
        }
        Hub.names[signal].push({callback: callback, self: self});
    },

    send: function() {
        /*
        Send a signal to the Gtk side by changing document.title.

        For example:

        >>> Hub.send('changed', 'foo', 'bar');

        */
        var params = Array.prototype.slice.call(arguments);
        var signal = params[0];
        var args = params.slice(1);
        Hub._emit(signal, args);
        var obj = {
            'i': Hub.i,
            'signal': signal,
            'args': args,
        };
        Hub.i += 1;
        document.title = JSON.stringify(obj);
    },

    recv: function(data) {
        /*
        Gtk should call this function to emit a signal to JavaScript handlers.
        
        For example:

        >>> Hub.recv('{"signal": "changed", "args": ["foo", "bar"]}');

        If you need to emit a signal from JavaScript to JavaScript handlers,
        use Hub.emit() instead.
        */
        var obj = JSON.parse(data);
        Hub._emit(obj.signal, obj.args);
    },

    emit: function() {
        /*
        Emit a signal from JavaScript to JavaScript handlers.

        For example:

        >>> Hub.emit('changed', 'foo', 'bar');

        */
        var params = Array.prototype.slice.call(arguments);
        Hub._emit(params[0], params.slice(1));
    },

    _emit: function(signal, args) {
        /*
        Low-level private function to emit a signal to JavaScript handlers.
        */
        var handlers = Hub.names[signal];
        if (handlers) {
            handlers.forEach(function(h) {
                h.callback.apply(h.self, args);
            });
        }
    },
}


function $(id) {
    /*
    Return the element with id="id".

    If `id` is an Element, it is returned unchanged.

    Examples:

    >>> $('browser');
    <div id="browser" class="box">
    >>> var el = $('browser');
    >>> $(el);
    <div id="browser" class="box">

    */
    if (id instanceof Element) {
        return id;
    }
    return document.getElementById(id);
}
