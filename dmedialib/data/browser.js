
var data = [];

var selected = "none";

function load(){
	var images = "";
	for (item in data){
		images += '<div class="item"> <img src="data:';
		images += data[item]._attachments.thumbnail.content_type;
		images += ';base64,';
		images += data[item]._attachments.thumbnail.data;
		images += '" width="160" height="90" onClick="get_data(\'';
		images += data[item]._id;
		images += '\')" >';
		images += '<div class="time">' + data[item].duration + 's</div>';
		images += '<div class="star_off"></div><div class="star_on"></div></div>';
	};

	var browser = document.getElementById('browser');
	browser.innerHTML = images;
};

function get_data(id){
	//var info = document.getElementById('info');
	var preview = document.getElementById('display');
	if (selected != "none"){
		//if something is selected, save the changes to it's values before loading the next object
		form = document.forms[0];
		title = form.elements["title"];
		tags = form.elements["tags"];
		description = form.elements["description"];
		notes = form.elements["notes"];
		set_data(selected, "title", title.value);
		set_data(selected, "tags", tags.value);
		set_data(selected, "description", description.value);
		set_data(selected, "notes", notes.value);


	};
	selected = id;

	for (item in data){
		if (data[item]._id == id){
			content = "<h2>Video Info</h2><p>";
			content += "<b>File Name: </b>" + data[item].name + "<br>";
			content += "<b>FPS: </b>" + data[item].fps + "<br>";
			content += "<b>Aperture: </b>" + data[item].aperture + "<br>";
			content += "<b>Focal Length: </b>" + data[item].focal_length + "<br>";
			content += "<b>Shutter: </b>" + data[item].shutter + "<br>";
			content += "<b>Camera: </b>" + data[item].camera + "<br>";
			content += "<b>Video Codec: </b>" + data[item].codec_video + "<br>";
			content += "<b>Resolution: </b>" + data[item].width + "x" + data[item].height + "<br>";
			//info.innerHTML = content;

			content += "<img src=\"data:";
			content += data[item]._attachments.thumbnail.content_type;
			content += ";base64,";
			content += data[item]._attachments.thumbnail.data;
			content += "\" width=\"192\" height=\"108\" align=\"center\"><br>";

			content += "<form>";
			content += "<b>Title: </b><input type=\"text\" class=\"field\" name=\"title\" value=\"" + data[item].title + "\"><br>";
			content += "<div class=\"star " + data[item].rating + "\">" + data[item].rating + "</div>";
			content += "<b>Tags: </b><br><textarea class=\"field\" name=\"tags\" rows=\"5\" cols=\"34\">" + data[item].tags + "</textarea><br>";
			content += "<b>Description: </b><br><textarea class=\"field\" name=\"description\" rows=\"5\" cols=\"34\">" + data[item].description + "</textarea><br>";
			content += "<b>Notes: </b><br><textarea class=\"field\" name=\"notes\" rows=\"5\" cols=\"34\">" + data[item].notes + "</textarea><br>";
			content += "</form>";
			preview.innerHTML = content;

			oText = oForm.elements["tags"];
			document.write(oText.value);
		};
	};


};

function set_data(id, tag, value){
	for (item in data){
		if (data[item]._id == id){
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
