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

    on_doc: function(req) {
        var doc = req.read();
        var d = new Date(doc.ctime * 1000);
        var keys = ['camera', 'lens', 'aperture', 'shutter', 'iso'];
        keys.forEach(function(key) {
            $(key).textContent = doc.meta[key];
        });
    },

    play: function(id) {       
        if (UI.selected) {
            UI.selected.classList.remove('selected');
        }
        UI.selected = $(id);
        UI.selected.classList.add('selected');
        UI.player.src = UI.url + id;
        UI.player.load();
        UI.player.play();
        db.get(UI.on_doc, id);
    },

    next: function() {
        if (UI.selected && UI.selected.nextSibling) {
            UI.play(UI.selected.nextSibling.id);
        }
    },
}

window.onload = function() {
    UI.url = db.get_sync('_local/peers')['self'];
    UI.info = $('info');
    UI.player = $('player');
    UI.player.addEventListener('ended', function() {
        UI.next();
    });
    db.view(UI.on_view, 'user', 'video');
}
