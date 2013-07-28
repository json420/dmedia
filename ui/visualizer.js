"use strict";

var db = new couch.Database('dmedia-1');


var UI = {
    on_load: function() {
        console.log('on_load()');
        UI.viz = new Visualizer();
    },
}

window.addEventListener('load', UI.on_load);


function Visualizer() {
    this.machine_id = db.get_sync('_local/dmedia').machine_id;
    this.machine = db.get_sync(this.machine_id);
    console.log(JSON.stringify(this.machine));
}
Visualizer.prototype = {
    

}

