"use strict";


var db = new couch.Database('dmedia-0');

var UI = {
    init: function() {
        UI.projects = new Items('projects');
        db.view(UI.on_projects, 'project', 'title');
    },

    on_projects: function(req) {
        var rows = req.read().rows;
        UI.projects.replace(rows,
            function(row) {
                var li = $el('li', {'class': 'project', 'id': row.id});

                var thumb = $el('div', {'class': 'thumbnail'});
                thumb.style.backgroundImage = db.att_css_url(row.id);

                var info = $el('div', {'class': 'info'});
                info.appendChild(
                    $el('p', {'textContent': row.key, 'class': 'title'})
                );

                info.appendChild(
                    $el('p', {'textContent': format_date(row.value)})
                );

                info.appendChild(
                    $el('p', {'textContent': '38 files, 971 MB'})
                );
                
                li.appendChild(thumb);
                li.appendChild(info);
                
                li.onclick = function() {
                    UI.projects.select(row.id);
                }
                
                return li;
            }
        );
        UI.projects.select_first();
    },
}


window.onload = function() {
    UI.init();
}
