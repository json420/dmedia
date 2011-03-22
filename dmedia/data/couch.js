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
couch.CouchBase = function(url, Request) {
    this.url = url || '/';
    if (this.url[this.url.length - 1] != '/') {
        this.url = this.url + '/';
    }
    this.Request = Request || XMLHttpRequest;
}
couch.CouchBase.prototype = {
    post: function(obj, parts, options) {
        /*
        Do a POST request.

        Examples:

        var cb = new couch.CouchBase('/');
        cb.post(null, ['foo', '_compact']);  # compact db /foo
        cb.post({_id: 'bar'}, 'foo');  # create doc /foo/bar
        cb.post({_id: 'baz'}, 'foo', {batch: true});  # with query option

        */
        return this.request('POST', obj, parts, options);
    },

    put: function(obj, parts, options) {
        /*
        Do a PUT request.

        Examples:

        var cb = new couch.CouchBase('/');
        cb.put(null, 'foo');  # create db /foo
        cb.put({hello: 'world'}, ['foo', 'bar']);  # create doc /foo/bar
        cb.put({a: 1}, ['foo', 'baz'], {batch: true});  # with query option

        */
        return this.request('PUT', obj, parts, options);
    },

    get: function(parts, options) {
        return this.request('GET', null, parts, options);
    },

    delete: function(parts, options) {
        return this.request('DELETE', null, parts, options);
    },

    path: function(parts, options) {
        /*
        Construct a URL relative to this.url.

        Examples:

        var inst = new couch.CouchBase('/foo/');
        inst.parts() => '/foo/'
        inst.parts('bar') => '/foo/bar'
        inst.parts(['bar', 'baz']) => '/foo/bar/baz'
        */
        if (!parts) {
            var url = this.url;
        }
        else if (typeof parts == 'string') {
            var url = this.url + parts;
        }
        else {
            var url = this.url + parts.join('/');
        }
        if (options) {
            var keys = [];
            for (key in options) {
                keys.push(key);
            }
            keys.sort();
            var query = [];
            keys.forEach(function(key) {
                var value = options[key];
                query.push(
                    encodeURIComponent(key) + '=' + encodeURIComponent(value)
                );
            });
            return url + '?' + query.join('&');
        }
        return url;
    },

    request: function(method, obj, parts, options) {
        var url = this.path(parts, options);
        this.req = new this.Request();
        this.req.open(method, url, false);
        this.req.setRequestHeader('Accept', 'application/json');
        if (method == 'POST' || method == 'PUT') {
            this.req.setRequestHeader('Content-Type', 'application/json');
        }
        if (obj) {
            this.req.send(JSON.stringify(obj));
        }
        else {
            this.req.send();
        }
    },

}
