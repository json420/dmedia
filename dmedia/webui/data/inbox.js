"use strict";

var dmedia = {
    db: new couch.Database('dmedia', '/'),

    load: function() {
        var ret = dmedia.db.view('user', 'inbox', {
            include_docs: true,
            reduce: false,
            limit: 50,
        });
        var inbox = $el('div', {'id': 'inbox'});
        ret.rows.forEach(function(row) {
            var doc = row.doc;
            var box = $el('div', {'class': 'item'});
            box.textContent = doc.name;
            inbox.appendChild(box);
        });
        $replace('inbox', inbox);

    },
}

window.onload = dmedia.load
