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
        UI.player.src = UI.url + id;
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
        return;
        var div = $('drives');
        var drives = Object.keys(UI.drives);
        drives.sort();
        drives.forEach(function(drive) {
            console.log(drive);
            var dinfo = UI.drives[drive];
            div.appendChild(mk_drive(drive, dinfo));
            var partitions = Object.keys(dinfo.partitions);
            partitions.sort();
            partitions.forEach(function(partition) {
                console.log(partition);
                var pinfo = dinfo.partitions[partition];
                var stores = Object.keys(pinfo.stores);
                stores.sort();
                stores.forEach(function(store) {
                    console.log(store);
                });
            });
        });
    },

}

function mk_drive(id, info) {
    var div = $el('div', {id: id, textContent: info.text});
    return div;

}


window.onload = function() {
    UI.progressbar = new ProgressBar('progress');
    UI.total = $('total');
    UI.completed = $('completed');
    UI.cards = $('cards');
    UI.url = db.get_sync('_local/peers')['self'];
    UI.tabs = new Tabs();
    UI.tabs.show_tab('storage');    
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


UI.drives  = {
    "/org/freedesktop/UDisks/devices/sdb": {
        "bytes": 2000398934016, 
        "connection": "ata", 
        "internal": true, 
        "model": "WDC WD2002FAEX-007BA0", 
        "partition_scheme": "gpt", 
        "partitions": {
            "/org/freedesktop/UDisks/devices/sdb3": {
                "bytes": 1967398000128, 
                "filesystem": "ext4", 
                "filesystem_version": "1.0", 
                "label": "", 
                "mounts": [
                    "/home"
                ], 
                "number": 3, 
                "size": "1.97 TB", 
                "stores": {
                    "/home": {
                        "dmedia": true
                    }, 
                    "/home/jderose": {
                        "dmedia": true
                    }
                }, 
                "uuid": "419a4e98-6487-4b2a-9315-8256e97c08f3"
            }
        }, 
        "revision": "05.01D50", 
        "serial": "WD-WMAY02010850", 
        "size": "2 TB", 
        "text": "2 TB Drive"
    }, 
    "/org/freedesktop/UDisks/devices/sdc": {
        "bytes": 1000204886016, 
        "connection": "ata", 
        "internal": true, 
        "model": "WDC WD1002FBYS-02A6B0", 
        "partition_scheme": "mbr", 
        "partitions": {
            "/org/freedesktop/UDisks/devices/sdc1": {
                "bytes": 1000202241024, 
                "filesystem": "ext4", 
                "filesystem_version": "1.0", 
                "label": "dmedia1", 
                "mounts": [
                    "/srv/dmedia/dmedia1"
                ], 
                "number": 1, 
                "size": "1 TB", 
                "stores": {
                    "/srv/dmedia/dmedia1": {
                        "dmedia": true
                    }
                }, 
                "uuid": "3cee2c95-cd63-4990-ab80-0c96786c6efa"
            }
        }, 
        "revision": "03.00C60", 
        "serial": "WD-WMATV1598616", 
        "size": "1 TB", 
        "text": "1 TB Drive"
    }, 
    "/org/freedesktop/UDisks/devices/sdd": {
        "bytes": 1000204886016, 
        "connection": "ata", 
        "internal": true, 
        "model": "WDC WD1002FBYS-02A6B0", 
        "partition_scheme": "mbr", 
        "partitions": {
            "/org/freedesktop/UDisks/devices/sdd1": {
                "bytes": 1000202241024, 
                "filesystem": "ext4", 
                "filesystem_version": "1.0", 
                "label": "dmedia2", 
                "mounts": [
                    "/srv/dmedia/dmedia2"
                ], 
                "number": 1, 
                "size": "1 TB", 
                "stores": {
                    "/srv/dmedia/dmedia2": {
                        "dmedia": true
                    }
                }, 
                "uuid": "a3f9ea81-680f-4144-be3a-2616ac2a3c2c"
            }
        }, 
        "revision": "03.00C60", 
        "serial": "WD-WMATV1496290", 
        "size": "1 TB", 
        "text": "1 TB Drive"
    }, 
    "/org/freedesktop/UDisks/devices/sdf": {
        "bytes": 32017047552, 
        "connection": "usb", 
        "internal": false, 
        "model": "CF  USB_3_0 Read", 
        "partition_scheme": "mbr", 
        "partitions": {
            "/org/freedesktop/UDisks/devices/sdf1": {
                "bytes": 32017015296, 
                "filesystem": "vfat", 
                "filesystem_version": "FAT32", 
                "label": "EOS_DIGITAL", 
                "mounts": [
                    "/media/EOS_DIGITAL"
                ], 
                "number": 1, 
                "size": "32 GB", 
                "stores": {
                    "/media/EOS_DIGITAL": {}
                }, 
                "uuid": "2478-0E2D"
            }
        }, 
        "revision": "0545", 
        "serial": "000000003693", 
        "size": "32 GB", 
        "text": "32 GB Removable Drive"
    }
};









