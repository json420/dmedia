"use strict";

var db = new couch.Database('dmedia-0');

var UI = {

    init: function() {
        UI.content = $('content');
        UI.player = $el('video', {'id': 'player'});
        UI.player.addEventListener('click', UI.player_click);
        UI.content.appendChild(UI.player);
    },

    load_project: function() {
        $hide('picker');
        $show('browser');
        if (!UI.browser) {
            UI.init();
            UI.browser = new Browser(UI.picker.project, UI.player, 'right');
        }
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


function Picker(project) {
    this.items = new Items('projects');
    this.items.onchange = $bind(this.on_item_change, this);
    this.project = project;
    this.load_items();
}
Picker.prototype = {
    load_items: function() {
        var callback = $bind(this.on_items, this);
        db.view(callback, 'project', 'title');
    },

    on_items: function(req) {
        this.items.replace(req.read().rows,
            function(row, items) {
                var li = $el('li', {'class': 'project', 'id': row.id});

                var thumb = $el('div', {'class': 'thumbnail'});
                thumb.style.backgroundImage = db.att_css_url(row.id);

                var info = $el('div', {'class': 'info'});
                info.appendChild(
                    $el('p', {'textContent': row.key, 'class': 'title'})
                );

                info.appendChild(
                    $el('p', {'textContent': format_date(row.value)})
                );

                info.appendChild(
                    $el('p', {'textContent': '38 files, 971 MB'})
                );

                li.appendChild(thumb);
                li.appendChild(info);

                li.onclick = function() {
                    items.select(row.id);
                }

                return li;
            }
        );
    },

    on_item_change: function(id) {
        this.project.load(id);
        if (this.project.id) {
            UI.load_project();
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
                    //'textContent': row.id,
                }
            );
            child.onclick = function() {
                self.items.select(id);
            }
            child.src = self.project.att_url(row.id);
            
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
        var resolution = this.doc.width + 'x' + this.doc.height;
        var fnum = this.doc.framerate.num;
        var fdenom = this.doc.framerate.denom;
        var fps = Math.round((fnum/fdenom)*100)/100;
        
        $('clip_name').textContent = this.doc.name;
        $('clip_res').textContent = resolution;
        $('clip_len').textContent = this.doc.duration.seconds;
        $('clip_fps').textContent = fps + ' fps';
        
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
        this.items.parent.scrollLeft += delta;
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


window.onload = function() {
    UI.project = new Project(db);
    UI.picker = new Picker(UI.project);
}
