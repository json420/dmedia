"use strict";

var db = new couch.Database('dmedia');

var UI = {
    on_view: function(req) {
        var rows = req.read()['rows'];
        var tray = $('tray');
        rows.forEach(function(row) {
            var id = row.id;
            var img = $el('img',
                {
                    id: id,
                    src: db.att_url(id, 'thumbnail'),
                    width: 160,
                    height: 90,
                }
            );
            img._value = row.value;
            img.onclick = function() {
                UI.play(id);
            }
            tray.appendChild(img);
        });
    },

    play: function(id) {
        UI.player.pause();
        UI.player.src = '';
        UI.player.load();
        
        if (UI.selected) {
            UI.selected.classList.remove('selected');
        }
        UI.selected = $(id);
        UI.selected.classList.add('selected');

        var keys = [];
        var key;
        for (key in UI.selected._value) {
            keys.push(key);
        }
        var proxy = keys[0];
        console.log(id);
        console.log(proxy);
        
        UI.player.src = UI.url + proxy;
        UI.player.load();
        UI.player.play();
    },

    next: function() {
        if (UI.selected && UI.selected.nextSibling) {
            UI.play(UI.selected.nextSibling.id);
        }
    },
}

window.onload = function() {
    UI.url = db.get_sync('_local/peers')['self'];
    UI.player = $('player');
    UI.player.addEventListener('ended', function() {
        UI.next();
    });
    db.view(UI.on_view, 'user', 'video');
}
