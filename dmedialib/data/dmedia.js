var CouchDB = new Class({
    Implements: [Events, Options],

    options: {
        url: 'http://localhost:5984',
    },

    initialize: function(database, options) {
        console.log('CouchDB.initialize()');
        this.database = database;
        this.setOptions(options);
        this.baseurl = this.options.url + '/' + this.database;
        this.request = new Request({url: 'http://localhost:5984/dmedia/', method: 'GET'});
        this.request.addEvent('success', this.on_success.bind(this));
        this.request.addEvent('failure', this.on_failure.bind(this));
        this.request.addEvent('exception', this.on_exception.bind(this));
    },

    on_request: function() {
        console.log('on_request');
    },

    on_loadstart: function(event, xhr) {
        console.log('on_loadstart');
    },

    on_progress: function(event, xhr) {
        console.log('on_progress');
    },

    on_success: function(text, xml) {
        console.log('on_success');
        console.log('text: ', text);
        console.log('xml: ', xml);
    },

    on_failure: function(xhr) {
        console.log('on_failure');
    },

    on_exception: function(headerName, value) {
        console.log('on_exception: %s, %s', headerName, value);
    },

    info: function() {
        this.request.send();
    },
});


var Dmedia = {
    on_load: function() {
        console.log('on_load');
        $('button').addEvent('click', Dmedia.on_click);
        Dmedia.couch = new CouchDB('dmedia');
    },

    on_click: function() {
        console.log('on_click');
        Dmedia.couch.info();
    },
};

window.addEvent('load', Dmedia.on_load);
