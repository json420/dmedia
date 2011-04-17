"use strict";

function Inbox(id, db) {
    this.el = $(id);
    this.db = db;

    var self = this;
    window.addEventListener('keypress',
        function (event) {self.on_keypress(event)},
        true
    );

    this.load();
}
Inbox.prototype = {
    on_keypress: function(event) {
        if (event.charCode == 32) {
            // spacebar
            event.preventDefault();
            this.reject();
        }
        else if (event.keyCode == 13) {
            // enter
            event.preventDefault();
            this.keep();
        }
    },

    load: function() {
        var result = this.db.view('user', 'inbox', {
            include_docs: true,
            reduce: false,
            limit: 50,
        });
        var inbox = $el('div', {'id': 'inbox'});
        $appendEach(inbox, result, function(doc) {
            var box = $el('div', {
                'class': 'item',
                'id': doc._id,
                'textContent': doc.name,
            });
            box._doc = doc;
            return box;
        });
        $replace(this.el, inbox);
        this.el = inbox;

        if (this.el.childNodes.length > 0) {
            this.select(this.el.childNodes[0]);
        }
    },

    select: function(el) {
        if (this.selected) {
            this.selected.classList.remove('selected');
        }
        if (! el) {
            this.selected = null;
            return;
        }
        this.selected = el;
        this.selected.classList.add('selected');
        this.selected.scrollIntoView();
    },

    next: function() {
        if (this.selected) {
            this.select(this.selected.nextSibling);
        }
    },

    set_status: function(status) {
        if (! this.selected) {
            return;
        }
        this.selected.classList.add(status);
        this.selected._doc.status = status;
        this.db.save(this.selected._doc);
        this.next();
    },

    keep: function() {
        this.set_status('keep');
    },

    reject: function() {
        this.set_status('reject');
    },
}


var dmedia = {
    db: new couch.Database('dmedia', '/'),

    on_load: function() {
        dmedia.inbox = new Inbox('inbox', dmedia.db);
    },

}

window.addEventListener('load', dmedia.on_load, false);
