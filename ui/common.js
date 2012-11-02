"use strict";

/*
Common functions and classes.

Aside from defining said functions and classes, including this file should have
no other side effects.
*/


var MONTHS = [
    'Jan',
    'Feb',
    'Mar',
    'Apr',
    'May',
    'Jun',
    'Jul',
    'Aug',
    'Sep',
    'Oct',
    'Nov',
    'Dec',
];

var DAYS = [
    'Sun',
    'Mon',
    'Tue',
    'Wed',
    'Thu',
    'Fri',
    'Sat',
];

function format_date(ts) {
    var d = new Date(ts * 1000);
    return [
        DAYS[d.getDay()] + ',', 
        d.getDate(),
        MONTHS[d.getMonth()],
        d.getFullYear()
    ].join(' ');
}


function set_title(id, value) {
    var el = $(id);
    if (value) {
        el.textContent = value;
    }
    else {
        el.textContent = '';
        el.appendChild($el('em', {textContent: 'Untitled'}));
    }
    return el;
}


// FIXME: Move this to UserWebKit base.js
function $scroll_to(id) {
    var child = $(id);
    if (! (child && child.parentNode)) {
        return;
    }
    var start = child.offsetTop;
    var end = start + child.offsetHeight;
    var vis_start = child.parentNode.scrollTop;
    var vis_end = vis_start + child.parentNode.clientHeight;
    if (start < vis_start) {
        child.parentNode.scrollTop = start;
    }
    else if (end > vis_end) {
        child.parentNode.scrollTop = end - child.parentNode.clientHeight;
    }
}


function force_int(value) {
    if (typeof value == 'number' && value >= 0) {
        return value;
    }
    return 0;
}


function files(count) {
    if (count == 1) {
        return '1 file';
    }
    return count.toString() + ' files';
}


function count_n_size(count, size) {
    count = force_int(count);
    size = force_int(size);
    return [files(count), bytes10(size)].join(', ');
}
console.assert(count_n_size() == '0 files, 0 bytes');
console.assert(count_n_size(null, null) == '0 files, 0 bytes');
console.assert(count_n_size('foo', 'bar') == '0 files, 0 bytes');


function ProgressBar(id) {
    this.element = $(id);
    this._bar = this.element.getElementsByTagName('div')[0];
}
ProgressBar.prototype = {
    set progress(value) {
        this._progress = Math.max(0, Math.min(value, 1));
        this._bar.style.width = (this._progress * 100).toFixed(0) + '%';
    },

    update: function(completed, total) {
        if (total > 0) {
            this.progress = completed / total;
        }
        else {
            this.progress = 0;
        }
    },
}



function Project(coredb) {
    this.coredb = coredb;
}
Project.prototype = {
    load: function(id) {
        if (!id) {
            this.id = null;
            this.doc = null;
            this.db = null;
            return false;
        }
        this.id = id;
        this.doc = this.coredb.get_sync(id);
        this.db = new couch.Database(this.doc['db_name']);
        return true;
    },

    load_recent: function() {
        var rows = this.coredb.view_sync('project', 'atime',
               {limit: 1, descending: true}
        )['rows'];
        if (rows.length >= 1) {
            this.load(rows[0].id);
        }
        else {
            this.load(null);
        }
    },

    access: function() {
        /* Update the doc.atime timestamp */
        if (this.doc) {
            this.doc.atime = couch.time();
            this.coredb.save(this.doc);
        }
    },

    select: function(id) {
        if (this.load(id)) {
            this.access();
        }
    },

    att_url: function(doc_or_id, name) {
        if (!this.db) {
            return null;
        }
        return this.db.att_url(doc_or_id, name);
    },

    att_css_url: function(doc_or_id, name) {
        if (!this.db) {
            return null;
        }
        return this.db.att_css_url(doc_or_id, name);
    },
}



function Items(id) {
    this.parent = $(id);
    this.current = null;
    this.onchange = null;
}
Items.prototype = {
    set_current: function(value) {
        if (this.current !== value) {
            this.current = value;
            if (this.onchange) {
                this.onchange(value);
            }
        }
    },

    length: function() {
        return this.parent.children.length;
    },

    clear: function() {
        this.parent.innerHTML = null;    
    },

    reset: function() {
        this.parent.innerHTML = null;    
        this.current = null;
    },

    select: function(id) {
        $unselect(this.current);
        if ($select(id)) {
            this.set_current(id);
            $scroll_to(id);
            return true;
        }
        this.set_current(null);
        return false;
    },

    toggle: function(id) {
        if (this.current == id) {
            this.select(null);
        }
        else {
            this.select(id);
        }
    },

    reselect: function() {
        return this.select(this.current);
    },

    select_first: function() {
        if (this.parent.children.length > 0) {
            this.select(this.parent.children[0].id);
        }
    },

    select_last: function() {
        if (this.parent.children.length > 0) {
            var child = this.parent.children[this.parent.children.length - 1];
            this.select(child.id);
        }
    },

    next: function(wrap) {
        var element = $(this.current);
        if (element && element.nextSibling) {
            this.select(element.nextSibling.id);
        }
        else if (wrap) {
            this.select_first();
        }
    },

    previous: function(wrap) {
        var element = $(this.current);
        if (element && element.previousSibling) {
            this.select(element.previousSibling.id);
        }
        else if (wrap) {
            this.select_last();
        }
    },

    append_each: function(rows, callback) {
        rows.forEach(function(row) {
            var child = callback(row, this);
            this.parent.appendChild(child);
        }, this);
    },

    replace: function(rows, callback) {
        this.clear();
        this.append_each(rows, callback);
    },

}

function tag_value(tag) {
    return tag.trim().replace(/\s+/g, ' ');
}


function tag_key(tag) {
    return tag.replace(/[-\s_.,]+/g, '').toLowerCase();
}


function create_tag(tag) {
    return {
        '_id': couch.random_id(),
        'ver': 0,
        'type': 'dmedia/tag',
        'time': couch.time(),
        'value': tag_value(tag),
        'key': tag_key(tag),
    }
}


function Tagger(project, form) {
    this.key = null;
    this.old_value = '';
    this.req = null;
    this.ontag = null;

    this.project = project;

    this.form = $(form);
    this.input = this.form.getElementsByTagName('input')[0];
    this.button = this.form.getElementsByTagName('button')[0];
    var ul = this.form.getElementsByTagName('ul')[0];
    this.matches = new Items(ul);

    this.form.onsubmit = $bind(this.on_submit, this);

    this.input.onkeydown = $bind(this.on_keydown, this);
    this.input.oninput = $bind(this.on_input, this);
    this.input.onfocus = $bind(this.on_focus, this);
    this.input.onblur = $bind(this.on_blur, this);

    this.matches.onchange = $bind(this.on_change, this);
    
    this.focus();
}
Tagger.prototype = {
    focus: function() {
        this.input.focus();
    },  

    abort: function() {
        if (this.req) {
            this.req.req.abort();
            this.req.req = null;
            this.req = null;
        }  
    },

    reset: function() {
        this.abort();
        this.input.value = '';
        this.old_value = '';
        this.key = null;
        this.button.disabled = true;
        this.matches.reset();
    },

    search: function() {
        this.abort();
        if (!this.key) {
            this.matches.reset();
            return;
        }
        var callback = $bind(this.on_search, this);
        this.req = this.project.db.view(callback, 'tag', 'letters',
            {key: this.key, limit: 5}
        );
    },

    on_search: function(req) {
        var rows = req.read().rows;
        this.matches.reset();
        var self = this;
        this.matches.append_each(rows, 
            function(row) {
                var id = row.id;
                var child = $el('li', {id: id, textContent: row.value});
                child.onclick = function() {
                    self.matches.toggle(id);
                }
                return child;
            }
        );
    },

    on_keydown: function(event) {
        if (! (this.input.value && this.matches.length())) {
            return;
        }
        // U+0009 = Tab
        var keyID = event.keyIdentifier;
        if (['Up', 'Down', 'U+0009'].indexOf(keyID) > -1) {
            event.preventDefault();
            event.stopPropagation();
            if (keyID == 'Up') {
                this.matches.previous(true);
            }
            else {  // Down or Tab
                this.matches.next(true);
            }
        }
    },

    on_input: function(event) {
        this.button.disabled = (!this.input.value);
        this.old_value = this.input.value;
        var key = tag_key(this.input.value);
        if (key != this.key) {
            this.key = key;
            this.search();
        }
    },

    on_submit: function(event) {
        event.preventDefault();
        event.stopPropagation();
        this.choose();
    },

    choose: function() {
        if (!this.input.value) {
            return;
        }
        if (!this.matches.current) {
            var key = tag_key(this.input.value);
            var rows = this.project.db.view_sync('tag', 'key',
                {key: key, limit: 1, reduce: false}
            ).rows;
            if (rows.length > 0) {
                var doc = this.project.db.get_sync(rows[0].id);
            }
            else {         
                var doc = create_tag(this.input.value);
                this.project.db.save(doc);
            }
        }
        else {
            var doc = this.project.db.get_sync(this.matches.current);
        }
        this.reset();
        if (this.ontag) {
            this.ontag(doc);
        }
    },

    on_change: function(tag_id) {
        console.assert(tag_id == this.matches.current);
        if (!tag_id) {
            this.input.value = this.old_value;
            this.key = tag_key(this.input.value);
        }
        else {
            var doc = this.project.db.get_sync(tag_id);
            this.input.value = doc.value;
            this.key = doc.key;
        }
        this.focus();
    },

    on_focus: function(event) {
        this.isfocused = true;
    },

    on_blur: function(event) {
        this.isfocused = false;
    },
}


function Tabs() {
    function make_handler(element) {
        var id = element.id;
        return function(event) {
            window.location.hash = '#' + id;
        }
    }

    var elements = document.getElementsByClassName('tab');
    var i;
    for (i=0; i<elements.length; i++) {
        var element = elements[i];
        element.onclick = make_handler(element);
    }

    var self = this;
    window.addEventListener('hashchange', function() {
        self.on_hashchange();
    });
}
Tabs.prototype = {
    on_hashchange: function() {
        var id = window.location.hash.slice(1);
        this.show_tab(id);
    },

    show_tab: function(id) {
        if (this.tab) {
            this.tab.classList.remove('active'); 
        }
        this.tab = $(id);
        this.tab.classList.add('active');
        if (this.target) {
            this.target.classList.add('hide');
        }
        this.target = $(id + '_target');
        this.target.classList.remove('hide');
        Hub.emit('tab_changed', this, id);
    },
}










