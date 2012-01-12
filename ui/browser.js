"use strict";

var db = new couch.Database('dmedia-0-77liabvfvoppxooe2wpsozsq');


function css_url(url) {
    return ['url(', JSON.stringify(url), ')'].join('');
}

window.onload = function() {
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
            $select(id);
        }
        var url = db.att_url(row.id, 'thumbnail');
		child.style.backgroundImage = css_url(url);
        div.appendChild(child);
    });
}
