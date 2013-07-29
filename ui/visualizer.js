"use strict";




var UI = {
    on_load: function() {
        console.log('on_load()');
        UI.db = new couch.Database('dmedia-1');
        UI.viz = new Visualizer(UI.db);
    },
}

window.addEventListener('load', UI.on_load);


function Visualizer(db) {
    this.db = db;
    this.machine_id = db.get_sync('_local/dmedia').machine_id;
    this.changes = new Changes(db,
        this.on_load.bind(this),
        this.on_add.bind(this)
    );
    this.changes.start();
}
Visualizer.prototype = {
    on_load: function(doc) {
        console.log(['on_load', doc.type, doc._id].join(' '));
        if (doc._id == this.machine_id) {
            var widget = new MachineWidget(this.changes, doc);
            document.body.appendChild(widget.element);
        }
    },

    on_add: function(doc) {
        console.log(['on_add', doc.type, doc._id].join(' '));
        //console.log(JSON.stringify(doc));
    },
}


var Changes = function(db, onload, onadd) {
    this.db = db;
    this.onload = onload;
    this.onadd = onadd;
    this.ids = {};
    this.docs = {};
}
Changes.prototype = {
    get: function(_id) {
        if (!this.docs[_id]) {
            this.docs[_id] = this.db.get_sync(_id);
        }
        return this.docs[_id];
    },

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
        var options = {
            'update_seq': true,
            'include_docs': true,
        }
        this.db.view(this.on_docs.bind(this), 'viz', 'all', options);
    },

    on_docs: function(req) {
        var _id;
        var result = req.read();
        result.rows.forEach(function(row) {
            this.docs[row.id] = row.doc;
        }, this);
        if (this.onload) {
            for (_id in this.docs) {
                this.onload(this.docs[_id]);
            }
        }
        this.monitor(result.update_seq);
    },

    monitor: function(since) {
        var options = {
            'since': since,
            'feed': 'longpoll',
            'include_docs': true,
            'filter': 'viz/all',
        }
        this.db.get(this.on_changes.bind(this), '_changes', options);
    },

    on_changes: function(req) {
        console.log('on_changes');
        // Get all the docs into this.docs before calling any callbacks:
        var result = req.read();
        var new_docs = [];
        var changed_docs = [];
        result.results.forEach(function(row) {
            if (!this.ids[row.id]) {
                new_docs.push(row.doc);
            }
            else {
                changed_docs.push(row.doc);
            }
            this.docs[row.id] = row.doc;
        }, this);

        // Now call onadd() for new docs:
        if (this.onadd) {
            new_docs.forEach(function(doc) {
                this.onadd(doc);
            }, this);
        }

        // And call notify() changed docs:
        changed_docs.forEach(function(doc) {
            this.notify(doc);
        }, this);

        this.monitor(result.last_seq);
    },
}


var Widget = function(changes, doc) {
    this.changes = changes;
    this.element = this.build(doc._id);
    this.on_change(doc);
    changes.subscribe(doc._id, this.on_change, this);
}
Widget.prototype = {
    on_change: function(doc) {
        if (doc._deleted) {
            this.destroy();  
        }
        else {
            this.update(doc);
            this.doc = doc;
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
        delete this.changes;
        delete this.doc;
    },

    update: function(doc) {

    },
}


var MachineWidget = function(changes, doc) {
    Widget.call(this, changes, doc);
    this._url = null;
}
MachineWidget.prototype = {
    build: function(doc_id) {
        var element = $el('div', {'class': 'machine', 'id': doc_id});
        this.text = $el('div', {'class': 'text'});
        element.appendChild(this.text);
        this.drives = $el('div', {'class': 'drives'});
        element.appendChild(this.drives);
        return element;
    },

    update: function(doc) {
        this.text.textContent = doc.hostname;
    },
}
MachineWidget.prototype.__proto__ = Widget.prototype;

