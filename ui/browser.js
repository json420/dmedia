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
        UI.player.setAttribute('width', UI.content.clientWidth - 20);
        UI.player.setAttribute('height', UI.content.clientHeight - 20);
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

function make_tag_li(tag_id, tag) {
    var li = $el('li', {textContent: tag.value});
    li.appendChild($el('a', {href: '#', textContent: 'x'}));
    return li;
}


function Browser(player, items) {
    this.player = $(player);
    this.tags = $('tags');
    this.doc = null;
    
    this.items = new Items(items);
    this.items.onchange = $bind(this.on_item_change, this);
    
    this.project = new Project();
    
    this.tagger = new Tagger(this.project, 'tag_value', 'tag_matches');
    this.tagger.ontag = $bind(this.on_tag, this);
    
    this.load_items();
    
}
Browser.prototype = {
    load_items: function() {
        var callback = $bind(this.on_items, this);
        this.project.db.view(callback, 'user', 'video', {'reduce': false});
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
            child.style.backgroundImage = self.project.att_url(row.id);
            return child;
        }
        this.items.replace(req.read().rows, callback);
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
        keys.forEach(function(key) {
            this.tags.appendChild(make_tag_li(key, this.doc.tags[key]));
        }, this);
    },

    on_tag: function(tag) {
        console.log(tag.value);
        console.log(tag.key);
        console.log(tag._id);
        if (!this.doc) {
            return;
        }
        if (!this.doc.tags) {
            this.doc.tags = {};
        } 
        if (!this.doc.tags[tag._id]) {
            this.doc.tags[tag._id] = {key: tag.key, value: tag.value};
        }
        else {
            this.doc.tags[tag._id].key = tag.key;
            this.doc.tags[tag._id].value = tag.value;
        }
        this.project.db.save(this.doc);
    },
    
}




window.onload = function() {
    UI.init();
    UI.browser = new Browser(UI.player, 'right');
}
