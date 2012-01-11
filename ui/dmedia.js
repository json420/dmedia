"use strict";

var db = new couch.Database('dmedia-0');


function $bind(func, self) {
    return function() {
        var args = Array.prototype.slice.call(arguments);
        return func.apply(self, args);
    }
}


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


var B32ALPHABET = '234567ABCDEFGHIJKLMNOPQRSTUVWXYZ';

function random_b32() {
    return B32ALPHABET[Math.floor(Math.random() * 32)];
}

function random_id(count) {    
    count = count || 24;
    var letters = [];
    var i;
    for (i=0; i<count; i++) {
        letters.push(random_b32());
    }
    return letters.join('');
}


function random_id2() {
    return [Math.floor(time()), random_id(16)].join('-');
}


function $select(id) {
    var element = $(id);
    if (element) {
        element.classList.add('selected');
    }
    return element;
}


function $unselect(id) {
    var element = $(id);
    if (element) {
        element.classList.remove('selected');
    }
    return element;
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

    on_projects_old: function(req) {
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
        UI.browser = new Browser();
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
    UI.tabs.show_tab('browser');    
}


function files(count) {
    if (count == 1) {
        return '1 file';
    }
    return count.toString() + ' files';
}


function bytes10(size) {
    /*
    Return *size* bytes to 3 significant digits in SI base-10 units.

    For example:
    >>> bytes10(1000);
    "1 kB"
    >>> bytes10(29481537)
    "29.5 MB"
    >>> bytes10(392012353)
    "392 MB"

    For additional details, see:

        https://wiki.ubuntu.com/UnitsPolicy
    */
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
        var end = s.slice(-1);
        while (end == '0' || end == '.') {
            s = s.slice(0, -1);
            if (end == '.') {
                break;
            }
            end = s.slice(-1);
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


function Project(id) {
    if (! this.load(id)) {
        this.load_recent();
    }
}
Project.prototype = {
    load: function(id) {
        if (!id) {
            this.id = null;
            this.doc = null;
            this.db = null;
            return false;
        }
        this.id = id;
        this.doc = db.get_sync(id);
        this.db = new couch.Database(this.doc['db_name']);
        return true;
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
        if (this.doc) {
            this.doc.atime = time();
            db.save(this.doc);
        }
    },

    select: function(id) {
        if (this.load(id)) {
            this.access();
        }
    },
}


function Items(id, callback, obj) {
    this.parent = $(id);
    this.callback = callback;
    this.obj = obj;
    this.current = null;
}
Items.prototype = {
    clear: function() {
        this.parent.innerHTML = null;    
    },

    reset: function() {
        this.parent.innerHTML = null;    
        this.current = null;
    },

    select: function(id) {
        $unselect(this.current);
        if ($select(id)) {
            this.current = id;
            return true;
        }
        this.current = null;
        return false;
    },

    reselect: function() {
        return this.select(this.current);
    },

    select_first: function() {
        if (this.parent.children.length > 0) {
            this.select(this.parent.children[0].id);
        }
    },

    select_last: function() {
        if (this.parent.children.length > 0) {
            var child = this.parent.children[this.parent.children.length - 1];
            this.select(child.id);
        }
    },

    next: function(wrap) {
        var element = $(this.current);
        if (element && element.nextSibling) {
            this.select(element.nextSibling.id);
        }
        else if (wrap) {
            this.select_first();
        }
    },

    previous: function(wrap) {
        var element = $(this.current);
        if (element && element.previousSibling) {
            this.select(element.previousSibling.id);
        }
        else if (wrap) {
            this.select_last();
        }
    },

    append_each: function(rows, callback) {
        rows.forEach(function(row) {
            var child = callback(row, this);
            this.parent.appendChild(child);
        }, this);
    },

}


function tags_to_string(tags) {
    if (!tags) {
        return '';
    }
    var keys = Object.keys(tags);
    keys.sort();
    var values = [];
    keys.forEach(function(key) {
        if (tags[key] && tags[key].value) {
            values.push(tags[key].value);
        }
        else {
             values.push(key);
        }
    });
    return values.join(', ');
}



function tag_value(tag) {
    return tag.trim().replace(/\s+/g, ' ');
}


function tag_key(value) {
    return value.replace(/[-_\s]/g, '').toLowerCase();
}


function string_to_tags(string, tags) {
    tags = tags || {};
    string.split(',').forEach(function(tag) {
        var value = tag_value(tag);
        var key = tag_key(value);
        if (!key) {
            return;
        }
        if (tags[key]) {
            tags[key].value = value;
        }
        else {
            tags[key] = {value: value};
        }
    });
    return tags;
}


function Tag(project, input, matches) {
    this.project = project;
    this.input = $(input);
    this.matches = new Items(matches);
    this.key = null;
    this.req = null;
    this.input.onkeydown = $bind(this.on_keydown, this);
    this.input.onkeyup = $bind(this.on_keyup, this);
    this.input.onchange = $bind(this.on_change, this);
}
Tag.prototype = {
    abort: function() {
        if (this.req) {
            this.req.req.abort();
            this.req.req = null;
            this.req = null;
        }  
    },

    search: function() {
        console.log(this.key);
        this.abort();
        if (!this.key) {
            this.matches.reset();
            return;
        }
        var callback = $bind(this.on_search, this);
        this.req = this.project.db.view(callback, 'tag', 'letters',
            {key: this.key, limit: 5}
        );
    },

    on_search: function(req) {
        var rows = req.read().rows;
        this.matches.clear();
        this.matches.append_each(rows, 
            function(row) {
                return $el('li', {id: row.id, textContent: row.value});
            }
        );
        if (! this.matches.reselect()) {
            this.matches.select_first();
        }
    },

    on_keydown: function(event) {
        var keyID = event.keyIdentifier;
        if (keyID == 'Up' || keyID == 'Down') {
            event.preventDefault();
            if (keyID == 'Up') {
                this.matches.previous(true);
            }
            else {
                this.matches.next(true);
            }      
        }
    },

    on_keyup: function(event) {
        var key = tag_key(this.input.value);
        if (key != this.key) {
            this.key = key;
            this.search();
        }
    },

    on_change: function() {
        if (this.onactivate) {
            this.key = tag_key(this.input.value);
            this.onactivate(this.key, this.input.value);
        }
    },
}


function Browser() {
    this.select = $('browser_projects');
    this.player = $('player');
    this.items = new Items('tray', this.select_item, this);

    var self = this;
    this.select.onchange = function() {
        self.on_change();
    }
    this.player.addEventListener('ended',
        function() {
            self.on_ended();
        }
    );

    this.doc = null;

    this.project = new Project();
    this.load_projects();

    this.tag = new Tag(this.project, 'tag', 'tag_matches');
}
Browser.prototype = {
    load_projects: function() {
        var self = this;
        var callback = function(req) {
            self.on_projects(req);
        }
        db.view(callback, 'project', 'title');
    },

    on_projects: function(req) {
        this.select.innerHTML = null;
        var rows = req.read()['rows'];
        rows.forEach(function(row) {
            var option = $el('option', {value: row.id});
            set_title(option, row.key);
            this.select.appendChild(option);
        }, this);
        this.select.value = this.project.id;
        this.load_items();
    },

    load_items: function() {
        return;
        var self = this;
        var callback = function(req) {
            self.on_items(req);
        }
        this.project.db.view(callback, 'user', 'video', {reduce: false});
    },

    select_item: function(id) {
        this.player.src = 'dmedia:' + id;
        this.player.load();
        this.project.db.get($bind(this.on_doc, this), id);
    },

    on_doc: function(req) {
        var doc = req.read();
        return;
        if (this.doc) {
            this.doc.tags = string_to_tags($('tags').value);
            this.project.db.save(this.doc);
        }
        this.doc = req.read();
        $('tags').value = tags_to_string(this.doc.tags);
    },

    play: function(id) {
        console.log(id);
        this.player.src = 'dmedia:' + id;
        this.player.play();
    },

    next: function() {
        if (UI.selected && UI.selected.nextSibling) {
            UI.play(UI.selected.nextSibling.id);
            UI.selected.scrollIntoView(false);
        }
    },

    on_change: function() {
        this.player.pause();
        this.player.src = null;
        this.project.select(this.select.value);
        this.load_items();
    },

    on_ended: function() {
        console.log('on_ended');
        console.log(this.project.id);
    },

    on_items: function(req) {
        var rows = req.read()['rows'];
        var project = this.project;
        this.items.clear();
        this.items.append_each(rows,
            function(row, items) {
                var id = row.id;
                var img = $el('img',
                    {
                        id: id,
                        src: project.db.att_url(id, 'thumbnail'),
                    }
                );
                img.onclick = function() {
                    items.select(id);
                }
                return img;
            }
        );
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








