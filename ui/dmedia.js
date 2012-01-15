var db = new couch.Database('dmedia-0');

var UI = {
    project: null,

    select_project: function(_id) {
        var selected = $(UI.project);
        if (selected) {
            selected.classList.remove('selected');
        }
        UI.project = _id;
        if (_id) {
            $(_id).classList.add('selected');
            $('start_importer').disabled = false;
        }
        else {
            $('start_importer').disabled = true;
        }
    },

    start_importer: function() {
        if (UI.project) {
            Hub.send('start_importer', UI.project);
        }
    },

    reload_projects: function() {
        db.view(UI.on_projects, 'project', 'title');
    },

    on_projects: function(req) {
        var rows = req.read()['rows'];
        var div = $('projects');
        div.textContent = '';
        rows.forEach(function(row) {
            var _id = row.id;
            var p = $el('p', {'id': _id, 'class': 'project'});
            set_title(p, row.key);
            p.onclick = function() {
                UI.select_project(_id);
            }
            div.appendChild(p);
        });
        var selected = $(UI.project);
        if (selected) {
            selected.classList.add('selected');
        }
    },

    on_projects_old: function(req) {
        var rows = req.read()['rows'];
        var div = $('projects');
        div.textContent = '';
        rows.forEach(function(row) {
            var _id = row.id;
            var p = $el('p', {'id': _id, 'class': 'project'});
            set_title(p, row.key);
            p.onclick = function() {
                UI.select_project(_id);
            }
            div.appendChild(p);
        });
        var selected = $(UI.project);
        if (selected) {
            selected.classList.add('selected');
        }
    },

    on_view: function(req) {
        var rows = req.read()['rows'];
        var tray = $('tray');
        rows.forEach(function(row) {
            var id = row.id;
            var img = $el('img',
                {
                    id: id,
                    src: db.att_url(id, 'thumbnail'),
                }
            );
            img.onclick = function() {
                UI.play(id);
            }
            tray.appendChild(img);
        });
    },

    play: function(id) {
        if (UI.selected) {
            UI.selected.classList.remove('selected');
        }
        UI.selected = $(id);
        UI.selected.classList.add('selected');
        UI.player.pause();
        UI.player.src = '';
        db.get(UI.on_doc, id);
        UI.player.src = 'dmedia:' + id;
        UI.player.load();
        UI.player.play();
    },

    next: function() {
        if (UI.selected && UI.selected.nextSibling) {
            UI.play(UI.selected.nextSibling.id);
            UI.selected.scrollIntoView(false);
        }
    },

    create_project: function() {
        UI.select_project(null);
        var title = $('project_title').value;
        Hub.send('create_project', title);
    },

    tabinit: {},    

    on_tab_changed: function(tabs, id) {
        if (!UI.tabinit[id]) {
            UI.tabinit[id] = true;
            UI['init_' + id]();
        }
    },

    init_import: function() {
        UI.select_project(null);
        UI.reload_projects();
    },

    init_history: function() {
        console.log('init_history'); 
    },

    init_browser: function() {
        UI.browser = new Browser();
    },

    init_storage: function() {
        console.log('init_storage'); 
    },
  
}



window.onload = function() {
    UI.progressbar = new ProgressBar('progress');
    UI.total = $('total');
    UI.completed = $('completed');
    UI.cards = $('cards');
    UI.tabs = new Tabs();
    UI.tabs.show_tab('import');    
}



// Lazily init-tabs so startup is faster, more responsive
Hub.connect('tab_changed', UI.on_tab_changed);


// Creating projects
Hub.connect('project_created',
    function(_id, title) {
        console.log(_id);
        console.log(title);
        UI.project = _id;
        UI.reload_projects();
    }
);


Hub.connect('importer_started',
    function(project_db_name) {
        UI.pdb = new couch.Database(project_db_name);
        $hide('choose_project');
        $show('importer');
    }
);

Hub.connect('importer_stopped',
    function() {
        $hide('importer');
        $show('choose_project');
    }
);


// All the import related signals:
Hub.connect('batch_started',
    function(batch_id) {
        $hide('summary');
        $show('info');
        UI.cards.textContent = '';
        UI.total.textContent = '';
        UI.completed.textContent = '';
        UI.progressbar.progress = 0;
    }
);

Hub.connect('batch_progress',
    function(count, total_count, size, total_size) {
        UI.total.textContent = count_n_size(total_count, total_size);
        UI.completed.textContent = count_n_size(count, size);
        UI.progressbar.update(size, total_size);
    }
);

Hub.connect('import_started',
    function(basedir, import_id, info) {
        var div = $el('div', {'id': import_id, 'class': 'thumbnail'});
        var inner = $el('div');
        div.appendChild(inner);

        var label = $el('p', {'class': 'card-label'});
        label.textContent = [
            bytes10(info.partition.bytes),
            info.partition.label
        ].join(', ');
        inner.appendChild(label);

        var info = $el('p', {textContent: '...'});
        inner.appendChild(info);
        div._info = info;

        UI.cards.appendChild(div);
    }
);

Hub.connect('import_scanned',
    function(basedir, import_id, total_count, total_size) {
        $(import_id)._info.textContent = count_n_size(total_count, total_size);
    }
);

Hub.connect('import_thumbnail',
    function(basedir, import_id, doc_id) {
        var url = UI.pdb.att_url(doc_id, 'thumbnail');
        $(import_id).style.backgroundImage = 'url("' + url + '")'; 
    }
);

Hub.connect('batch_finalized',
    function(batch_id, stats, copies, msg) {
        $hide('info');
        $show('summary');
        $('summary_summary').textContent = msg[0];
        var body = $('summary_body');
        body.textContent = '';
        msg.slice(1).forEach(function(line) {
            body.appendChild(
                $el('p', {textContent: line})
            );
        });
    }
);

