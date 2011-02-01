
var selected = "none";

function load(rows) {
    var browser = document.getElementById('browser');
    for (i in rows) {
        var doc = rows[i].doc;

        var div = document.createElement('div');
        div.setAttribute('class', 'item');

        var img = document.createElement('img');
        if (doc._attachments.thumbnail) {
            img.setAttribute('src', '/dmedia/' + doc._id + '/thumbnail');
        }
        img.setAttribute('width', 160);
        img.setAttribute('height', 90);
        img.onclick = function(){get_data(doc)};

        var time = document.createElement('div');
        time.setAttribute('class', 'time');
        time.textContent = doc.duration + 's';

        var star_off = document.createElement('div');
        star_off.setAttribute('class', 'star_off');

        var star_on = document.createElement('div');
        star_on.setAttribute('class', 'star_on');

        div.appendChild(img);
        div.appendChild(time);
        div.appendChild(star_off);
        div.appendChild(star_on);
        browser.appendChild(div);
    };
};

function get_data(doc){
    //console.log('get_data', doc);

//	if (selected != "none"){
//		//if something is selected, save the changes to it's values before loading the next object
//		form = document.forms[0];
//		title = form.elements["title"];
//		tags = form.elements["tags"];
//		description = form.elements["description"];
//		notes = form.elements["notes"];
//		set_data(selected, "title", title.value);
//		set_data(selected, "tags", tags.value);
//		set_data(selected, "description", description.value);
//		set_data(selected, "notes", notes.value);
//	};
//	selected = doc._id;

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


$.couch.urlPrefix = "";

var dmedia = {
    db: $.couch.db('dmedia'),

    data: [],

    callback: function(stuff) {
        //console.log(stuff);
        load(stuff.rows);
    },

    load: function() {

        dmedia.db.view('file/ext', {
            success: dmedia.callback,
            include_docs: true,
            key: 'mov',
            reduce: false
        });
    }
};
