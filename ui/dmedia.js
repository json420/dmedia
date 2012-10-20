"use strict";

var db = new couch.Database('dmedia-0');

var UI = {
    tabinit: {},

    on_load: function() {
        UI.progressbar = new ProgressBar('progress');
        UI.total = $('total');
        UI.completed = $('completed');
        UI.cards = $('cards');
        UI.tabs = new Tabs();
        UI.tabs.show_tab('import');
    },    

    on_tab_changed: function(tabs, id) {
        if (!UI.tabinit[id]) {
            UI.tabinit[id] = true;
            UI['init_' + id]();
        }
        if (UI.player){
            UI.player.pause();
        }
    },

    init_import: function() {
        UI.importer = new Importer();
    },

    init_history: function() {
        console.log('init_history'); 
    },

    init_browser: function() {
        UI.player = $el('video', {'id': 'player'});
        UI.player.addEventListener('click', UI.player_click);
        $("content").appendChild(UI.player);
        UI.browser = new Browser(UI.importer.project, UI.player, 'right');
        //console.log('init_browser');
    },

    init_storage: function() {
        console.log('init_storage'); 
    },

    player_click: function(event){
        if (!UI.player.paused) {
            UI.player.pause();
        }
        else {
            UI.player.play();
        }
    },
  
}

window.onload = UI.on_load;


function make_tag_li(remove, doc, id) {
    var id = id || doc._id;
    var li = $el('li', {textContent: doc.value});
    var a = $el('a', {textContent: 'x', title: 'Click to remove tag'});
    li.appendChild(a);
    a.onclick = function() {
        remove(id, li);
    }
    return li;
}


function wheel_delta(event) {
    var delta = event.wheelDeltaY;
    if (delta == 0) {
        return 0;
    }
    var scale = (event.shiftKey) ? -10 : -1;
    return scale * (delta / Math.abs(delta));
}


function set_flag(div, review) {
    if (!review) {
        return;
    }
    div.appendChild(
        $el('img', {'src': review + '.png'})
    );
}


function reset_flag(id, review) {
    var div = $(id);
    if (!div) {
        return;
    }
    div.innerHTML = null;
    set_flag(div, review);
}


var Widget = function(session, doc) {
    this.session = session;
    this.element = this.build(doc._id);
    this.on_change(doc);
    session.subscribe(doc._id, this.on_change, this);
}
Widget.prototype = {
    on_change: function(doc) {
        if (doc._deleted) {
            this.destroy();  
        }
        else {
            this.update(doc);
        }
    },

    build: function(doc_id) {
        var element = document.createElement('div');
        element.setAttribute('id', doc_id);
        return element;
    },

    destroy: function() {
        if (this.element.parentNode) {
            this.element.parentNode.removeChild(this.element);
        }
        delete this.element;
        delete this.session;
    },

    update: function(doc) {

    },
}


var ProjectButton = function(session, doc) {
    Widget.call(this, session, doc);
}
ProjectButton.prototype = {
    build: function(doc_id) {
        var element = $el('li', {'class': 'project', 'id': doc_id});
        element.onclick = function() {
            Hub.send('start_importer', doc_id);
        }
        this.thumbnail = element.appendChild(
            $el('div', {'class': 'thumbnail'})
        );
        var info = element.appendChild(
            $el('div', {'class': 'info'})
        );
        this.title = info.appendChild(
            $el('p', {'class': 'title'})
        );
        this.date = info.appendChild($el('p'));
        this.stats = info.appendChild($el('p'));
        return element;
    },

    update: function(doc) {
        this.thumbnail.style.backgroundImage = this.session.thumbnail(doc);
        this.title.textContent = doc.title;
        this.date.textContent = format_date(doc.time);
        this.stats.textContent = count_n_size(doc.count, doc.bytes);
    },
}
ProjectButton.prototype.__proto__ = Widget.prototype;


var Projects = function(db, onload, onadd) {
    this.db = db;
    this.onload = onload;
    this.onadd = onadd;
    this.ids = {};
    this.docs = {};
}
Projects.prototype = {
    subscribe: function(_id, callback, self) {
        if (! this.ids[_id]) {
            this.ids[_id] = [];
        }
        this.ids[_id].push({callback: callback, self: self});
    },

    notify: function(doc) {
        var handlers = this.ids[doc._id];
        if (handlers) {
            handlers.forEach(function(h) {
                h.callback.call(h.self, doc);
            });
            if (doc._deleted) {
                delete this.ids[doc._id];
            }
        }
    },

    start: function() {
        var self = this;
        var on_docs = function(req) {
            self.on_docs(req);
        }
        this.db.view(on_docs, 'project', 'title',
            {include_docs: true, update_seq: true}
        );
    },

    on_docs: function(req) {
        var result = req.read();
        result.rows.forEach(function(row) {
            this.docs[row.id] = row.doc;
        }, this);
        var _id;
        for (_id in this.docs) {
            this.onload(this.docs[_id]);
        }
        this.monitor(result.update_seq);
    },

    monitor: function(since) {
        var options = {
            'since': since,
            'feed': 'longpoll',
            'include_docs': true,
            'filter': 'project/type',
        };
        var self = this;
        var callback = function(req) {
            self.on_changes(req);
        }
        this.db.get(callback, '_changes', options);
    },

    on_changes: function(req) {
        var result = req.read();
        result.results.forEach(function(row) {
            if (!this.docs[row.id]) {
                this.onadd(row.doc);
            }
            else {
                this.notify(row.doc);
            }
            this.docs[row.id] = row.doc;
        }, this);
        this.monitor(result.last_seq);
    },

    thumbnail: function(doc) {
        if (doc._attachments && doc._attachments.thumbnail) {
            return this.db.att_css_url(doc._id);
        }
        return null;
    },
}


function Importer() {
    this.create_button = $('create_project');

    this.start_button = $('start_importer');
    this.start_button.onclick = $bind(this.start_importer, this);

    this.input = $('project_title');
    this.input.oninput = $bind(this.on_input, this);
    $('project_form').onsubmit = $bind(this.on_submit, this);

    this.projects = $('projects');

    var on_load = $bind(this.on_project_load, this);
    var on_add = $bind(this.on_project_add, this);
    this.session = new Projects(db, on_load, on_add);
    this.session.start();
}
Importer.prototype = {
    on_project_load: function(doc) {
        var widget = new ProjectButton(this.session, doc);
        this.projects.appendChild(widget.element);
    },

    on_project_add: function(doc) {
        var widget = new ProjectButton(this.session, doc);
        widget.element.classList.add('added');
        this.projects.insertBefore(widget.element, this.projects.children[0]);
    },

    on_input: function() {
        this.create_button.disabled = (!this.input.value);
    },

    on_submit: function(event) {
        event.preventDefault();
        event.stopPropagation();
        if (!this.input.value) {
            return;
        }
        this.create_button.disabled = true;
        Hub.send('create_project', this.input.value);
        this.input.value = '';
    },

    start_importer: function() {
        if (this.project.id) {
            this.project.access();
            Hub.send('start_importer', this.project.id);
        }
    },

}

function Browser(project, player, items) {
    this.player = $(player);
    this.tags = $('tags');
    this.doc = null;

    this.items = new Items(items);
    this.items.onchange = $bind(this.on_item_change, this);
    this.items.parent.onmousewheel = $bind(this.on_mousewheel, this);

    this.project = project;

    this.tagger = new Tagger(this.project, 'tag_form');
    this.tagger.ontag = $bind(this.on_tag, this);

    window.addEventListener('keydown', $bind(this.on_window_keydown, this));
    window.addEventListener('keypress', $bind(this.on_window_keypress, this));

    this.load_items();
}
Browser.prototype = {
    _review: function(value) {
        this.doc.review = value;
        this.project.db.save(this.doc);
        reset_flag(this.doc._id, value);
    },

    accept: function() {
        if (!this.doc) {
            return;
        }
        this._review('accept');
    },

    reject: function() {
        if (!this.doc) {
            return;
        }
        this._review('reject');
        this.next();
    },

    next_needing_review: function() {
        var rows = this.project.db.view_sync('user', 'video_needsreview', {'limit': 1}).rows;
        if (rows.length == 1) {
            this.items.select(rows[0].id);
        }
    },

    next: function() {
        this.items.next();
    },

    previous: function() {
        this.items.previous();
    },

    load_items: function() {
        var callback = $bind(this.on_items, this);
        this.project.db.view(callback, 'user', 'video');
    },

    on_items: function(req) {
        var self = this;
        var callback = function(row) {
            var id = row.id;
            var child = $el('img',
                {
                    'class': 'thumbnail',
                    'id': row.id,
                    'src': self.project.att_url(row.id),
                    //'textContent': row.id,
                }
            );
            child.onclick = function() {
                self.items.select(id);
            }
            //child.style.backgroundImage = self.project.att_css_url(row.id);
            
            child.draggable = true;
            child.ondragstart = function(e) {
                e.dataTransfer.effectAllowed = 'copy';
                e.dataTransfer.setData('Text', self.project.db.name + "/" + id);
                var img = $el('img', {'src': self.project.att_url(id)});
                e.dataTransfer.setDragImage(img, 0, 0);
            }
                
            if (row.value) {
                set_flag(child, row.value);
            }
            return child;
        }
        this.items.replace(req.read().rows, callback);
        this.next_needing_review();
    },

    on_item_change: function(id) {
        if (!id) {
            this.doc = null;
            this.player.pause();
            this.player.src = null;
            return;
        }
        this.player.src = 'dmedia:' + id;
        this.player.play();
        this.tagger.reset();
        this.tags.innerHTML = null;
        this.project.db.get($bind(this.on_doc, this), id);
    },

    on_doc: function(req) {
        this.doc = req.read();
        var keys = Object.keys(this.doc.tags);
        var remove = $bind(this.on_tag_remove, this);
        keys.forEach(function(key) {
            this.tags.appendChild
                (make_tag_li(remove, this.doc.tags[key], key)
            );
        }, this);
    },

    on_tag: function(tag) {
        //console.log(tag);
        if (!this.doc) {
            return;
        }
        if (!this.doc.tags) {
            this.doc.tags = {};
        } 
        if (!this.doc.tags[tag._id]) {
            var remove = $bind(this.on_tag_remove, this);
            $prepend(make_tag_li(remove, tag), this.tags);
            this.doc.tags[tag._id] = {key: tag.key, value: tag.value};
        }
        else {
            this.doc.tags[tag._id].key = tag.key;
            this.doc.tags[tag._id].value = tag.value;
        }
        this.project.db.save(this.doc);
    },

    on_tag_remove: function(id, li) {
        this.tags.removeChild(li);
        if (this.doc && this.doc.tags) {
            delete this.doc.tags[id];
            this.project.db.save(this.doc);
        }
    },

    on_mousewheel: function(event) {
        event.preventDefault();
        var delta = wheel_delta(event) * 112;  // 108px height + 2px border
        this.items.parent.scrollTop += delta;
    },


    on_window_keydown: function(event) {
        var keyID = event.keyIdentifier;
        if (['Up', 'Down', 'Enter', 'U+007F'].indexOf(keyID) > -1 && !this.tagger.input.value) {
            event.preventDefault();
            event.stopPropagation();
            if (keyID == 'Up') {
                this.previous();
            }
            else if (keyID == 'Down') {
                this.next();
            }
            else if (keyID == 'Enter') {
                this.accept();
            }
            else { // Delete
                this.reject();
            }
        }
    },

    on_window_keypress: function(event) {
        //console.log(['window keypress', event.which, event.keyCode].join(', '));
        if (this.tagger.isfocused) {
            return;   
        }
        // Don't focus on Backspace, Enter, Spacebar, or Delete
        if ([8, 13, 32, 127].indexOf(event.keyCode) == -1) {
            this.tagger.focus();
        }
    },

}



// Lazily init-tabs so startup is faster, more responsive
Hub.connect('tab_changed', UI.on_tab_changed);



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

