"use strict";

var dmedia = {
    db: new couch.Database('dmedia', '/'),

    load: function() {
        var result = dmedia.db.view('user', 'inbox', {
            include_docs: true,
            reduce: false,
            limit: 10,
        });
        var inbox = $el('div', {'id': 'inbox'});
        $appendEach(inbox, result, function(doc) {
            var box = $el('div', {'class': 'item'});
            $appendMeta(box, data.meta, doc.meta, function(label, value) {
                var p = $el('p');
                p.appendChild($el('strong', {textContent: label + ': '}));
                p.appendChild($el('span', {textContent: value}));
                return p;
            });
            return box;
        });
        $replace('inbox', inbox);

    },
}

window.onload = dmedia.load
