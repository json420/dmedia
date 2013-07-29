"use strict";


function $unparent(id) {
    var child = $(id);
    if (child && child.parentNode) {
        child.parentNode.removeChild(child);
    }
    return child;
}


var UI = {
    on_load: function() {
        console.log('on_load()');
        UI.db = new couch.Database('visualizer-1');
        UI.machines = $('machines');
        UI.machine_id = UI.db.get_sync('_local/dmedia').machine_id;
        UI.viz = new Visualizer(UI.db);
    },
}

window.addEventListener('load', UI.on_load);


function Visualizer(db) {
    this.db = db;
    this.changes = new Changes(db,
        this.on_load.bind(this),
        this.on_add.bind(this)
    );
    this.changes.start();
}
Visualizer.prototype = {
    on_load: function(doc) {
        console.log(['on_load', doc.type, doc._id].join(' '));
        if (doc._id == UI.machine_id) {
            var widget = new MachineWidget(this.changes, doc);
            $prepend(widget.element, UI.machines);
        }
    },

    on_add: function(doc) {
        console.log(['on_add', doc.type, doc._id].join(' '));
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
                delete this.docs[doc._id];
            }
        }
    },

    notify_delete: function(doc_id) {
        var doc = {
            '_id': doc_id,
            '_deleted': true,
        }
        this.notify(doc);
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
        console.log('destroy: ' + this.element.id);
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
}
MachineWidget.prototype = {
    destroy: function() {
        console.log('destroy machine: ' + this.element.id);
        var deleted = [];
        var child = this.drives.children[0];
        while (child) {
            deleted.push(child.id);
            child = child.nextSibling;
        }
        deleted.forEach(function(doc_id) {
            console.log('deleting drive: ' + doc_id);
            this.changes.notify_delete(doc_id);
        }, this);

        delete this.drives;
        delete this.text;
        if (this.element.parentNode) {
            this.element.parentNode.removeChild(this.element);
        }
        delete this.element;
        delete this.changes;
        delete this.doc;
    },

    build: function(doc_id) {
        var element = $el('div', {'class': 'machine', 'id': doc_id});
        this.text = $el('div', {'class': 'text'});
        element.appendChild(this.text);
        this.drives = $el('div', {'class': 'drives'});
        element.appendChild(this.drives);
        return element;
    },

    update: function(doc) {
        console.log(JSON.stringify(doc));
        this.text.textContent = doc.hostname;

        var store_id, peer_id, child, child_doc, widget;
        for (store_id in doc.stores) {
            child = $(store_id);
            if (!child) {
                child_doc = this.changes.get(store_id);
                widget = new DriveWidget(this.changes, child_doc);
                child = widget.element;
                this.drives.appendChild(child);
            }
        }
        var deleted = [];
        child = this.drives.children[0];
        while (child) {
            if (!doc.stores[child.id]) {
                deleted.push(child.id);
                console.log('deleting drive: ' + child.id);
            }
            child = child.nextSibling;
        }
        deleted.forEach(function(doc_id) {
            this.changes.notify_delete(doc_id);
        }, this);

        if (doc._id != UI.machine_id) {
            return;
        }

        for (peer_id in doc.peers) {
            console.log(peer_id);
            child = $(peer_id);
            if (!child) {
                child_doc = this.changes.get(peer_id);
                widget = new MachineWidget(this.changes, child_doc);
                child = widget.element;
                UI.machines.appendChild(child);
            }
        }
        var deleted = [];
        child = UI.machines.children[0];
        while (child) {
            if (!doc.peers[child.id] && child.id != UI.machine_id) {
                deleted.push(child.id);
                console.log('deleting peer: ' + child.id);
            }
            child = child.nextSibling;
        }
        deleted.forEach(function(doc_id) {
            this.changes.notify_delete(doc_id);
        }, this);
    },
}
MachineWidget.prototype.__proto__ = Widget.prototype;


var DriveWidget = function(changes, doc) {
    Widget.call(this, changes, doc);
}
DriveWidget.prototype = {
    build: function(doc_id) {
        var element = $el('div', {'class': 'drive', 'id': doc_id});
        return element;
    },

    update: function(doc) {
        this.element.textContent = [doc.drive_size, doc.drive_model].join(', ');
    },
}
DriveWidget.prototype.__proto__ = Widget.prototype;

