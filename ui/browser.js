"use strict";

var db = new couch.Database('dmedia-0-77liabvfvoppxooe2wpsozsq');


function css_url(url) {
    return ['url(', JSON.stringify(url), ')'].join('');
}


var UI = {
    timeout_id: null,

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
        UI.content.removeChild(UI.player);
        UI.resize();
        UI.content.appendChild(UI.player);
    },
}

window.onload = function() {
    UI.init();
    var div = $('right');
    div.innerHTML = null;
    var rows = db.view_sync('user', 'video', {reduce: false, limit: 100})['rows'];
    rows.forEach(function(row) {
        var id = row.id;
        var child = $el('div',
            {
                'class': 'thumbnail',
                'id': row.id,
            }
        );
        child.onclick = function() {
            UI.player.src = 'dmedia:' + id;
            UI.player.load();
        }
        var url = db.att_url(row.id, 'thumbnail');
		child.style.backgroundImage = css_url(url);
        div.appendChild(child);
    });
}
