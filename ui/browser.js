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


function Browser(player, items) {
    this.player = $(player);
    this.items = new Items(items);
    this.project = new Project();
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


}




window.onload = function() {
    UI.init();
    UI.browser = new Browser(UI.player, 'right');
}
