"use strict";

function $(id) {
    /*
    Return the element with id="id".

    If id is an Element, it is returned unchanged.

    Examples:

    >>> $('browser');
    <div id="browser" class="box">
    >>> var el = $('browser');
    undefined
    >>> $(el);
    <div id="browser" class="box">

    */
    if (id instanceof Element) {
        return id;
    }
    return document.getElementById(id);
}


function $el(tag, attributes) {
    /*
    Convenience function to create a new DOM element and set its attributes.

    Examples:

    >>> $el('img');
    <img>
    >>> $el('img', {'class': 'thumbnail', 'src': 'foo.png'});
    <img class="thumbnail" src="foo.png">

    */
    var el = document.createElement(tag);
    if (attributes) {
        var key;
        for (key in attributes) {
            el.setAttribute(key, attributes[key]);
        }
    }
    return el;
}


function Browser(id, db) {
    this.el = $(id);
    this.db = db;
    this.display = $('display');
}
Browser.prototype = {
    run: function() {
        var r = this.db.view('file', 'ext',
            {key: 'mov', reduce: false, include_docs: true}
        );
        this.load(r.rows);
    },

    load: function(rows) {
        rows.forEach(function(r) {
            var self = this;
            var doc = r.doc;

            var div = $el('div', {'class': 'item'});
            var img = $el('img', {'width': 160, 'height': 90});
            if (doc._attachments.thumbnail) {
                img.setAttribute('src', this.db.path([doc._id, 'thumbnail']));
            }
            img.onclick = function() {
                self.get_data(doc);
            };

            var time = $el('div', {'class': 'time'});
            time.textContent = doc.duration + 's';

            div.appendChild(img);
            div.appendChild(time);
            div.appendChild($el('div', {'class': 'star_off'}));
            div.appendChild($el('div', {'class': 'star_on'}));
            this.el.appendChild(div);
        }, this);
    },

    get_data: function(doc) {
        var names = ['name', 'iso', 'aperture', 'shutter', 'focal_length',
            'lens', 'camera'];
        names.forEach(function(n) {
            var el = $('meta.' + n);
            if (el) {
                el.textContent = doc[n];
            }
        });
    },


}

var selected = "none";



function get_data(doc){
    //console.log('get_data', doc);

//    if (selected != "none"){
//        //if something is selected, save the changes to it's values before loading the next object
//        form = document.forms[0];
//        title = form.elements["title"];
//        tags = form.elements["tags"];
//        description = form.elements["description"];
//        notes = form.elements["notes"];
//        set_data(selected, "title", title.value);
//        set_data(selected, "tags", tags.value);
//        set_data(selected, "description", description.value);
//        set_data(selected, "notes", notes.value);
//    };
//    selected = doc._id;

    var preview = document.getElementById('display');
    var content = "<h2>Video Info</h2><p>";
    content += "<b>File Name: </b>" + doc.basename + "<br>";
    content += "<b>FPS: </b>" + doc.fps + "<br>";
    content += "<b>Aperture: </b>" + doc.aperture + "<br>";
    content += "<b>Focal Length: </b>" + doc.focal_length + "<br>";
    content += "<b>Shutter: </b>" + doc.shutter + "<br>";
    content += "<b>Camera: </b>" + doc.camera + "<br>";
    content += "<b>Video Codec: </b>" + doc.codec_video + "<br>";
    content += "<b>Resolution: </b>" + doc.width + "x" + doc.height + "<br>";
    //info.innerHTML = content;

    content += "<img src=\"data:";
    content += doc._attachments.thumbnail.content_type;
    content += ";base64,";
    content += doc._attachments.thumbnail.data;
    content += "\" width=\"192\" height=\"108\" align=\"center\"><br>";

    content += "<form>";
    content += "<b>Title: </b><input type=\"text\" class=\"field\" name=\"title\" value=\"" + doc.title + "\"><br>";
    content += "<div class=\"star " + doc.rating + "\">" + doc.rating + "</div>";
    content += "<b>Tags: </b><br><textarea class=\"field\" name=\"tags\" rows=\"5\" cols=\"34\">" + doc.tags + "</textarea><br>";
    content += "<b>Description: </b><br><textarea class=\"field\" name=\"description\" rows=\"5\" cols=\"34\">" + doc.description + "</textarea><br>";
    content += "<b>Notes: </b><br><textarea class=\"field\" name=\"notes\" rows=\"5\" cols=\"34\">" + doc.notes + "</textarea><br>";
    content += "</form>";
    preview.innerHTML = content;

    //oText = oForm.elements["tags"];
    //document.write(oText.value);


};

function set_data(id, tag, value){
    return;
    for (item in dmedia.data){
        if (dmedia.data[item]._id == id){
            eval("data[item]." + tag + " = \"" + value + "\"")
        };
    };
};

function search_for(string){
    confirm("Really " + string + "?");
};

function close_box(){
    var box = document.getElementById('info');
    var dim = document.getElementById('dim');

    box.className += " out";
    dim.className = "out";
};


var dmedia = {
    db: new couch.Database('/dmedia/'),

    data: [],

    load: function() {
        dmedia.browser = new Browser('browser', dmedia.db);
        dmedia.browser.run();
    },
}
