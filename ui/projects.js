"use strict";


var db = new couch.Database('dmedia-0');


var UI = {
    init: function() {
        UI.projects = $('projects');
        db.view(UI.on_projects, 'project', 'title');
    },

    on_projects: function(req) {
        var rows = req.read().rows;
        rows.forEach(function(row) {
            var li = $el('li', {'class': 'project', 'id': row.id});

            var thumb = $el('div', {'class': 'thumbnail'});
            thumb.style.backgroundImage = db.att_css_url(row.id);

            var info = $el('div', {'class': 'info'});
            info.appendChild(
                $el('p', {'textContent': row.key, 'class': 'title'})
            );

            var d = new Date(row.value * 1000);
            info.appendChild(
                $el('p', {'textContent': d.toLocaleDateString()})
            );
            
            info.appendChild(
                $el('p', {'textContent': '38 files, 971 MB'})
            );
            
            li.appendChild(thumb);
            li.appendChild(info);
            UI.projects.appendChild(li);
        });
    },
}


window.onload = function() {
    UI.init();
}
