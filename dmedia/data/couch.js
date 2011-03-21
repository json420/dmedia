/*
JavaScript port of Python3 `microfiber` CouchDB adapter:

    https://launchpad.net/microfiber

    http://bazaar.launchpad.net/~microfiber/microfiber/trunk/view/head:/microfiber.py

Rather than inventing an API, this is a simple adapter for calling a REST JSON
API like CouchDB.  The goal is to make something that doesn't need constant
maintenances as additional features are added to the CouchDB API.

For some good documentation of the CouchDB REST API, see:

    http://docs.couchone.com/couchdb-api/
*/

var couch = {};

couch.stuff = function() {
    return 'Woot!';
}

couch.awesome = function(person) {
    return person + ' is awesome!';
}

// microfiber.CouchBase
couch.CouchBase = function(url) {
    this.url = url;
}
couch.CouchBase.prototype = {
    post: function(obj, parts, options) {

    },

    put: function(obj, parts, options) {

    },

    get: function(parts, options) {

    },

    delete: function(parts, options) {

    },

    head: function(parts, options) {

    },

    put_att: function(mime, data, parts, options) {

    },

    get_att: function(parts, options) {

    },
}
