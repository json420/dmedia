"use strict";


var UI = {

    init: function() {
        UI.content = $('content');
        UI.player = $el('video', {'id': 'player'});
        UI.resize();
        UI.content.appendChild(UI.player);
        window.addEventListener('resize', UI.on_resize);
    },

    resize: function() {
        UI.player.setAttribute('width', UI.content.clientWidth);
        UI.player.setAttribute('height', UI.content.clientHeight);
    },

    on_resize: function() {
        var paused = UI.player.paused;
        if (!paused) {
            UI.player.pause();
        }
        UI.content.removeChild(UI.player);
        UI.resize();
        UI.content.appendChild(UI.player);
        if (!paused) {
            UI.player.load();
            UI.player.play();
        }
    },
}


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




function Browser(player, items) {
    this.player = $(player);
    this.tags = $('tags');
    this.doc = null;

    this.items = new Items(items);
    this.items.onchange = $bind(this.on_item_change, this);
    this.items.parent.onmousewheel = $bind(this.on_mousewheel, this);

    this.project = new Project();

    this.tagger = new Tagger(this.project, 'tag_value', 'tag_matches');
    this.tagger.ontag = $bind(this.on_tag, this);

    $('tag_button').onclick = $bind(this.tagger.choose, this.tagger);

    window.addEventListener('keydown', $bind(this.on_window_keydown, this));
    window.addEventListener('keypress', $bind(this.on_window_keypress, this));

    this.load_items();
}
Browser.prototype = {
    _review: function(value) {
        this.doc.review = value;
        this.project.db.save(this.doc);
        reset_flag(this.doc._id, value);
        this.next_needing_review();
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
            var child = $el('div',
                {
                    'class': 'thumbnail',
                    'id': row.id,
                    //'textContent': row.id,
                }
            );
            child.onclick = function() {
                self.items.select(id);
            }
            child.style.backgroundImage = self.project.att_css_url(row.id);
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
        if (['Up', 'Down', 'Enter', 'U+0008', 'U+007F'].indexOf(keyID) > -1 && !this.tagger.input.value) {
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
            else {  // Backspace or Delete
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




window.onload = function() {
    UI.init();
    UI.browser = new Browser(UI.player, 'right');
}
