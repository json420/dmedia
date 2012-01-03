"use strict";

var db = new couch.Database('dmedia-0');


function set_title(id, value) {
    var el = $(id);
    if (value) {
        el.textContent = value;
    }
    else {
        el.textContent = '';
        el.appendChild($el('em', {textContent: 'Untitled'}));
    }
    return el;
}


function time() {
    /* Return Unix-style timestamp like time.time() */
    return Date.now() / 1000;
}


var UI = {
    project: null,

    select_project: function(_id) {
        var selected = $(UI.project);
        if (selected) {
            selected.classList.remove('selected');
        }
        UI.project = _id;
        if (_id) {
            $(_id).classList.add('selected');
            $('start_importer').disabled = false;
        }
        else {
            $('start_importer').disabled = true;
        }
    },

    start_importer: function() {
        if (UI.project) {
            Hub.send('start_importer', UI.project);
        }
    },

    reload_projects: function() {
        db.view(UI.on_projects, 'project', 'title');
    },

    on_projects: function(req) {
        var rows = req.read()['rows'];
        var div = $('projects');
        div.textContent = '';
        rows.forEach(function(row) {
            var _id = row.id;
            var p = $el('p', {'id': _id, 'class': 'project'});
            set_title(p, row.key);
            p.onclick = function() {
                UI.select_project(_id);
            }
            div.appendChild(p);
        });
        var selected = $(UI.project);
        if (selected) {
            selected.classList.add('selected');
        }
    },

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
            UI.selected.scrollIntoView(false);
        }
    },

    create_project: function() {
        UI.select_project(null);
        var title = $('project_title').value;
        Hub.send('create_project', title);
    },

    tabinit: {},    

    on_tab_changed: function(tabs, id) {
        if (!UI.tabinit[id]) {
            UI.tabinit[id] = true;
            UI['init_' + id]();
        }
    },

    init_import: function() {
        UI.select_project(null);
        UI.reload_projects();
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
    var s = ((i > 0) ? size / Math.pow(1000, i) : size).toPrecision(3);
    if (s.indexOf('.') > 0) {
        while (s.slice(-1) == '0' || s.slice(-1) == '.') {
            if (s.slice(-1) == '.') {
                s = s.slice(0, -1);
                break;
            }
            else {
                s = s.slice(0, -1);
            }
        }
    }
    return s + ' ' + BYTES10[i];
}

console.assert(bytes10(100 * 1000) == '100 kB');
console.assert(bytes10(10 * 1000) == '10 kB');
console.assert(bytes10(1000) == '1 kB');
console.assert(bytes10(123 * 1000) == '123 kB');
console.assert(bytes10(123 * 100) == '12.3 kB');
console.assert(bytes10(123 * 10) == '1.23 kB');
console.assert(bytes10(120 * 1000) == '120 kB');
console.assert(bytes10(120 * 100) == '12 kB');
console.assert(bytes10(120 * 10) == '1.2 kB');


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


function Project(_id) {
    if (! _id) {
        this.load_recent();
    }
    else {
        this.load(_id);
    }
}
Project.prototype = {
    load: function(_id) {
        this._id = _id;
        if (!_id) {
            this.doc = null;
            this.db = null;
        }
        else {
            this.doc = db.get_sync(_id);
            console.log(this.doc.title);
            this.db = new couch.Database(this.doc['db_name']);
        }
    },

    load_recent: function() {
        var rows = db.view_sync('project', 'atime', {limit: 1, descending: true})['rows'];
        if (rows.length >= 1) {
            this.load(rows[0].id);
        }
        else {
            this.load(null);
        }
    },

    access: function() {
        /* Update the doc.atime timestamp */
        if (this.doc && this.db) {
            this.doc.atime = time();
            this.db.save(this.doc);
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
        Hub.emit('tab_changed', this, id);
    },
}


// Lazily init-tabs so startup is faster, more responsive
Hub.connect('tab_changed', UI.on_tab_changed);


// Creating projects
Hub.connect('project_created',
    function(_id, title) {
        console.log(_id);
        console.log(title);
        UI.project = _id;
        UI.reload_projects();
    }
);


Hub.connect('importer_started',
    function(project_db_name) {
        UI.pdb = new couch.Database(project_db_name);
        $hide('choose_project');
        $show('importer');
    }
);

Hub.connect('importer_stopped',
    function() {
        $hide('importer');
        $show('choose_project');
    }
);


// All the import related signals:
Hub.connect('batch_started',
    function(batch_id) {
        $hide('summary');
        $show('info');
        UI.cards.textContent = '';
        UI.total.textContent = '';
        UI.completed.textContent = '';
        UI.progressbar.progress = 0;
    }
);

Hub.connect('batch_progress',
    function(count, total_count, size, total_size) {
        UI.total.textContent = count_n_size(total_count, total_size);
        UI.completed.textContent = count_n_size(count, size);
        UI.progressbar.update(size, total_size);
    }
);

Hub.connect('import_started',
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

Hub.connect('import_scanned',
    function(basedir, import_id, total_count, total_size) {
        $(import_id)._info.textContent = count_n_size(total_count, total_size);
    }
);

Hub.connect('import_thumbnail',
    function(basedir, import_id, doc_id) {
        var url = UI.pdb.att_url(doc_id, 'thumbnail');
        $(import_id).style.backgroundImage = 'url("' + url + '")'; 
    }
);

Hub.connect('batch_finalized',
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








