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


Signal.connect('batch_started',
    function(batch_id) {
        UI.cards.textContent = '';
        UI.new_row();
        UI.i = 0;
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
        if (UI.i > 0 && UI.i % 4 == 0) {
            UI.new_row();   
        }
        UI.i += 1;

        var div = $el('div', {'id': import_id, 'class': 'three'});

        var img = $el('img', {'src': '#'});
        div.appendChild(img);
        div._img = img;

        var label = $el('div', {'class': 'card-label'});
        label.textContent = [
            bytes10(info.partition.bytes),
            info.partition.label
        ].join(', ');
        div.appendChild(label);

        var info = $el('div');
        div.appendChild(info);
        div._info = info;

        UI.row.appendChild(div);
    }
);

Signal.connect('import_scanned',
    function(basedir, import_id, total_count, total_size) {
        $(import_id)._info.textContent = count_n_size(total_count, total_size);
    }
);


// The `dmedia` CouchDB database:
var db = new couch.Database('dmedia');


var UI = {
    new_row: function() {
        UI.row = $el('div', {'class': 'row'});
        UI.cards.appendChild(UI.row);
    },
};

window.onload = function() {
    UI.progressbar = new ProgressBar('progress');
    UI.total = $('total');
    UI.completed = $('completed');
    UI.cards = $('cards');
}


