var db = new couch.Database('dmedia-0');

var UI = {
    tabinit: {},    

    on_tab_changed: function(tabs, id) {
        if (!UI.tabinit[id]) {
            UI.tabinit[id] = true;
            UI['init_' + id]();
        }
    },

    init_import: function() {
        UI.importer = new Importer();
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


function Importer() {
    this.create_button = $('create_project');

    this.start_button = $('start_importer');
    this.start_button.onclick = $bind(this.start_importer, this);

    this.input = $('project_title');
    this.input.oninput = $bind(this.on_input, this);
    $('project_form').onsubmit = $bind(this.on_submit, this);

    this.items = new Items('projects');
    this.items.onchange = $bind(this.on_item_change, this);
    this.project = new Project(db);

    Hub.connect('project_created', $bind(this.on_project_created, this));

    this.load_items();
}
Importer.prototype = {
    load_items: function() {
        var callback = $bind(this.on_items, this);
        db.view(callback, 'project', 'title');
    },

    on_items: function(req) {
        this.items.replace(req.read().rows,
            function(row, items) {
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
                    items.toggle(row.id);
                }

                return li;
            }
        );
        this.items.select(this.project.id);
    },

    on_item_change: function(id) {
        this.project.load(id);
        this.start_button.disabled = (!this.project.id);
        if (this.project.doc) {
            $('target_project').textContent = this.project.doc.title;
        }
    },

    on_input: function() {
        this.create_button.disabled = (!this.input.value);
    },

    on_submit: function(event) {
        event.preventDefault();
        event.stopPropagation();
        if (!this.input.value) {
            return;
        }
        this.items.select(null);
        Hub.send('create_project', this.input.value);
        this.input.value = '';
        this.create_button.disabled = true;
    },

    on_project_created: function(id, title) {
        this.project.load(id);
        this.load_items();
    },

    start_importer: function() {
        if (this.project.id) {
            this.project.access();
            Hub.send('start_importer', this.project.id);
        }
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

