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


//window.onload = function() {
//    UI.progressbar = new ProgressBar('progress');
//    UI.total = $('total');
//    UI.completed = $('completed');
//    UI.cards = $('cards');
//    UI.tabs = new Tabs();
//    UI.tabs.show_tab('browser');    
//}


function files(count) {
    if (count == 1) {
        return '1 file';
    }
    return count.toString() + ' files';
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


function css_url(url) {
    return ['url(', JSON.stringify(url), ')'].join('');
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
            this.doc.atime = couch.time();
            db.save(this.doc);
        }
    },

    select: function(id) {
        if (this.load(id)) {
            this.access();
        }
    },
 
    att_url: function(doc_or_id, name) {
        if (!this.db) {
            return null;
        }
        name = name || 'thumbnail';
        return css_url(this.db.att_url(doc_or_id, name));
    },
}



function Items(id) {
    this.parent = $(id);
    this._current = null;
    this.onchange = null;
}
Items.prototype = {
    set current(value) {
        if (this._current !== value) {
            this._current = value;
            if (this.onchange) {
                this.onchange(value);
            }
        }
    },

    get current(value) {
        return this._current;
    },

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

    toggle: function(id) {
        if (this.current == id) {
            this.select(null);
        }
        else {
            this.select(id);
        }
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

    replace: function(rows, callback) {
        this.clear();
        this.append_each(rows, callback);
    },

}

function tag_value(tag) {
    return tag.trim().replace(/\s+/g, ' ');
}


function tag_key(tag) {
    return tag.replace(/[-\s_.,]+/g, '').toLowerCase();
}


function create_tag(tag) {
    return {
        '_id': couch.random_id(),
        'ver': 0,
        'type': 'dmedia/tag',
        'time': couch.time(),
        'value': tag_value(tag),
        'key': tag_key(tag),
    }
}


function Tagger(project, input, matches) {
    this.project = project;
    this.input = $(input);
    this.matches = new Items(matches);
    this.key = null;
    this.req = null;
    this.input.onkeydown = $bind(this.on_keydown, this);
    this.input.onkeyup = $bind(this.on_keyup, this);
    this.input.onchange = $bind(this.on_change, this);

    this.ontag = null;
}
Tagger.prototype = {
    abort: function() {
        if (this.req) {
            this.req.req.abort();
            this.req.req = null;
            this.req = null;
        }  
    },

    reset: function() {
        this.abort();
        this.input.value = '';
        this.key = null;
        this.matches.reset();
    },

    search: function() {
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
        this.matches.reset();
        this.matches.append_each(rows, 
            function(row) {
                return $el('li', {id: row.id, textContent: row.value});
            }
        );
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
        if (!this.matches.current) {
            var key = tag_key(this.input.value);
            var rows = this.project.db.view_sync('tag', 'key',
                {key: key, limit: 1, reduce: false}
            ).rows;
            if (rows.length > 0) {
                var doc = this.project.db.get_sync(rows[0].id);
            }
            else {         
                var doc = create_tag(this.input.value);
                this.project.db.save(doc);
            }
        }
        else {
            var doc = this.project.db.get_sync(this.matches.current);
        }
        this.reset();
        if (this.ontag) {
            this.ontag(doc);
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


//// Lazily init-tabs so startup is faster, more responsive
//Hub.connect('tab_changed', UI.on_tab_changed);


//// Creating projects
//Hub.connect('project_created',
//    function(_id, title) {
//        console.log(_id);
//        console.log(title);
//        UI.project = _id;
//        UI.reload_projects();
//    }
//);


//Hub.connect('importer_started',
//    function(project_db_name) {
//        UI.pdb = new couch.Database(project_db_name);
//        $hide('choose_project');
//        $show('importer');
//    }
//);

//Hub.connect('importer_stopped',
//    function() {
//        $hide('importer');
//        $show('choose_project');
//    }
//);


//// All the import related signals:
//Hub.connect('batch_started',
//    function(batch_id) {
//        $hide('summary');
//        $show('info');
//        UI.cards.textContent = '';
//        UI.total.textContent = '';
//        UI.completed.textContent = '';
//        UI.progressbar.progress = 0;
//    }
//);

//Hub.connect('batch_progress',
//    function(count, total_count, size, total_size) {
//        UI.total.textContent = count_n_size(total_count, total_size);
//        UI.completed.textContent = count_n_size(count, size);
//        UI.progressbar.update(size, total_size);
//    }
//);

//Hub.connect('import_started',
//    function(basedir, import_id, info) {
//        var div = $el('div', {'id': import_id, 'class': 'thumbnail'});
//        var inner = $el('div');
//        div.appendChild(inner);

//        var label = $el('p', {'class': 'card-label'});
//        label.textContent = [
//            bytes10(info.partition.bytes),
//            info.partition.label
//        ].join(', ');
//        inner.appendChild(label);

//        var info = $el('p', {textContent: '...'});
//        inner.appendChild(info);
//        div._info = info;

//        UI.cards.appendChild(div);
//    }
//);

//Hub.connect('import_scanned',
//    function(basedir, import_id, total_count, total_size) {
//        $(import_id)._info.textContent = count_n_size(total_count, total_size);
//    }
//);

//Hub.connect('import_thumbnail',
//    function(basedir, import_id, doc_id) {
//        var url = UI.pdb.att_url(doc_id, 'thumbnail');
//        $(import_id).style.backgroundImage = 'url("' + url + '")'; 
//    }
//);

//Hub.connect('batch_finalized',
//    function(batch_id, stats, copies, msg) {
//        $hide('info');
//        $show('summary');
//        $('summary_summary').textContent = msg[0];
//        var body = $('summary_body');
//        body.textContent = '';
//        msg.slice(1).forEach(function(line) {
//            body.appendChild(
//                $el('p', {textContent: line})
//            );
//        });
//    }
//);








