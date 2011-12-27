"use strict";

var db = new couch.Database('dmedia');

var UI = {
    on_doc: function(req) {
        var doc = req.read();
        var keys = ['camera', 'lens', 'aperture', 'shutter', 'iso'];
        keys.forEach(function(key) {
            var el = $(key);
            if (el) {
                el.textContent = doc.meta[key];
            }
        });
    },

    on_view: function(req) {
        var rows = req.read()['rows'];
        var tray = $('tray');
        rows.forEach(function(row) {
            var id = row.id;
            var img = $el('img',
                {
                    id: id,
                    src: db.att_url(id, 'thumbnail'),
                }
            );
            img.onclick = function() {
                UI.play(id);
            }
            tray.appendChild(img);
        });
    },

    play: function(id) {
        if (UI.selected) {
            UI.selected.classList.remove('selected');
        }
        UI.selected = $(id);
        UI.selected.classList.add('selected');
        UI.player.pause();
        UI.player.src = '';
        db.get(UI.on_doc, id);
        UI.player.src = 'dmedia:' + id;
        UI.player.load();
        UI.player.play();
    },

    next: function() {
        if (UI.selected && UI.selected.nextSibling) {
            UI.play(UI.selected.nextSibling.id);
        }
    },

    tabinit: {},    

    on_tab_changed: function(tabs, id) {
        if (!UI.tabinit[id]) {
            UI.tabinit[id] = true;
            UI['init_' + id]();
        }
    },

    init_import: function() {
        console.log('init_import');
    },

    init_history: function() {
        console.log('init_history'); 
    },

    init_browser: function() {
        UI.player = $('player');
        UI.player.addEventListener('ended', UI.next);
        db.view(UI.on_view, 'user', 'video', {reduce: false});
    },

    init_storage: function() {
        console.log('init_storage'); 
    },
    
}


window.onload = function() {
    UI.progressbar = new ProgressBar('progress');
    UI.total = $('total');
    UI.completed = $('completed');
    UI.cards = $('cards');
    UI.tabs = new Tabs();
    UI.tabs.show_tab('import');    
}


function $hide(id) {
    var element = $(id);
    element.classList.add('hide');
    return element;
}

function $show(id) {
    var element = $(id);
    element.classList.remove('hide');
    return element;
}


function files(count) {
    if (count == 1) {
        return '1 file';
    }
    return count.toString() + ' files';
}

function bytes10(size) {
    var BYTES10 = [
        'bytes',
        'kB',
        'MB',
        'GB',
        'TB',
        'PB',
        'EB',
        'ZB',
        'YB',
    ];
    if (size == 0) {
        return '0 bytes';
    }
    if (size == 1) {
        return '1 byte';
    }
    var ex = Math.floor(Math.log(size) / Math.log(1000));
    var i = Math.min(ex, BYTES10.length - 1);
    var s = (i > 0) ? size / Math.pow(1000, i) : size;
    return s.toPrecision(3) + ' ' + BYTES10[i];
}


function count_n_size(count, size) {
    return [files(count), bytes10(size)].join(', ');
}


function ProgressBar(id) {
    this.element = $(id);
    this._bar = this.element.getElementsByTagName('div')[0];
}
ProgressBar.prototype = {
    set progress(value) {
        var p = Math.max(0, Math.min(value, 1));
        this._bar.style.width = (p * 100).toFixed(0) + '%';
    },

    update: function(completed, total) {
        if (total > 0) {
            this.progress = completed / total;
        }
        else {
            this.progress = 0;
        }
    },

}


function Tabs() {
    function make_handler(element) {
        var id = element.id;
        return function(event) {
            window.location.hash = '#' + id;
        }
    }

    var elements = document.getElementsByClassName('tab');
    var i;
    for (i=0; i<elements.length; i++) {
        var element = elements[i];
        element.onclick = make_handler(element);
    }

    var self = this;
    window.addEventListener('hashchange', function() {
        self.on_hashchange();
    });
}

Tabs.prototype = {
    on_hashchange: function() {
        var id = window.location.hash.slice(1);
        this.show_tab(id);
    },

    show_tab: function(id) {
        if (this.tab) {
            this.tab.classList.remove('active'); 
        }
        this.tab = $(id);
        this.tab.classList.add('active');
        if (this.target) {
            this.target.classList.add('hide');
        }
        this.target = $(id + '_target');
        this.target.classList.remove('hide');
        Signal.emit('tab_changed', [this, id]);
    },
}




/*
Relay signals between JavaScript and Gtk.

For example, to send a signal to Gtk via document.title:

>>> Signal.send('click');
>>> Signal.send('changed', 'foo', 'bar');

*/
var Signal = {
    i: 0,

    names: {},

    connect: function(signal, callback, self) {
        if (! Signal.names[signal]) {
            Signal.names[signal] = [];
        }
        Signal.names[signal].push({callback: callback, self: self});
    },

    emit: function(signal, args) {
        var items = Signal.names[signal];
        if (items) {
            items.forEach(function(i) {
                i.callback.apply(i.self, args);
            });
        }
    },

    send: function() {
        var args = Array.prototype.slice.call(arguments);
        var obj = {
            i: Signal.i,
            signal: args[0],
            args: args.slice(1),
        };
        Signal.i += 1;
        document.title = JSON.stringify(obj);
    },

    recv: function(string) {
        var obj = JSON.parse(string);
        Signal.emit(obj.signal, obj.args);
    },

}


// Lazily init-tabs so startup is faster, more responsive
Signal.connect('tab_changed', UI.on_tab_changed);


// All the import related signals:
Signal.connect('batch_started',
    function(batch_id) {
        $hide('summary');
        $show('info');
        UI.cards.textContent = '';
        UI.total.textContent = '';
        UI.completed.textContent = '';
        UI.progressbar.progress = 0;
    }
);

Signal.connect('batch_progress',
    function(count, total_count, size, total_size) {
        UI.total.textContent = count_n_size(total_count, total_size);
        UI.completed.textContent = count_n_size(count, size);
        UI.progressbar.update(size, total_size);
    }
);

Signal.connect('import_started',
    function(basedir, import_id, info) {
        var div = $el('div', {'id': import_id, 'class': 'thumbnail'});
        var inner = $el('div');
        div.appendChild(inner);

        var label = $el('p', {'class': 'card-label'});
        label.textContent = [
            bytes10(info.partition.bytes),
            info.partition.label
        ].join(', ');
        inner.appendChild(label);

        var info = $el('p', {textContent: '...'});
        inner.appendChild(info);
        div._info = info;

        UI.cards.appendChild(div);
    }
);

Signal.connect('import_scanned',
    function(basedir, import_id, total_count, total_size) {
        $(import_id)._info.textContent = count_n_size(total_count, total_size);
    }
);

Signal.connect('thumbnail',
    function(basedir, import_id, doc_id) {
        var url = db.att_url(doc_id, 'thumbnail');
        $(import_id).style.backgroundImage = "url(\"" + url + "\")"; 
    }
);

Signal.connect('batch_finalized',
    function(batch_id, stats, copies, msg) {
        $hide('info');
        $show('summary');
        $('summary_summary').textContent = msg[0];
        var body = $('summary_body');
        body.textContent = '';
        msg.slice(1).forEach(function(line) {
            body.appendChild(
                $el('p', {textContent: line})
            );
        });
    }
);








