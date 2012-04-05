"use strict";


function open_project(project_id) {
    console.log(project_id);
    window.location.assign('browser.html#' + project_id); 
}


var UI = {
    init: function() {
        UI.db = new couch.Database("dmedia-0");
        UI.project = new Project(UI.db);
        UI.items = new Items('projects');
        
        UI.load_items();
    },

    load_items: function() {
        console.log('load_items');
        UI.db.view(UI.on_items, 'project', 'title');
    },

    on_items: function(req) {
        var rows = req.read().rows;
        console.log(rows.length);
        UI.items.replace(rows,
            function(row, items) {
                var pdb = new couch.Database("dmedia-0-" + row.id.toLowerCase());
                try{
                    var filecount = pdb.view_sync('doc', 'type', {key: 'dmedia/file'}).rows[0].value;
                }
                catch(e){
                    var filecount = 0;
                }
            
                var li = $el('li', {'class': 'project', 'id': row.id});

                var thumb = $el('div', {'class': 'thumbnail'});
                thumb.style.backgroundImage = UI.db.att_css_url(row.id);

                var info = $el('div', {'class': 'info'});
                info.appendChild(
                    $el('p', {'textContent': row.key, 'class': 'title'})
                );

                info.appendChild(
                    $el('p', {'textContent': format_date(row.value)})
                );

                info.appendChild(
                    $el('p', {'textContent': filecount + ' files'})
                );

                li.appendChild(thumb);
                li.appendChild(info);

                li.onclick = function() {
                    open_project(row.id);
                }

                return li;
            }
        );
        UI.items.select(UI.project.id);
    },
}


window.addEventListener('load', UI.init);
