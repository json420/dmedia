"use strict";

var dmedia = {
    db: new couch.Database('dmedia', '/'),

    load: function() {
        var result = dmedia.db.view('user', 'inbox', {
            include_docs: true,
            reduce: false,
            limit: 50,
        });
        var inbox = $el('div', {'id': 'inbox'});
        $appendEach(inbox, result, function(doc) {
            var box = $el('div', {'class': 'item'});
            box.textContent = doc.name;
            return box;
        });
        $replace('inbox', inbox);

    },
}

window.onload = dmedia.load
