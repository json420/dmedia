/*
JavaScript port of Python3 `microfiber` CouchDB adapter:

    https://launchpad.net/microfiber

    http://bazaar.launchpad.net/~microfiber/microfiber/trunk/view/head:/microfiber.py

Rather than inventing an API, this is a simple adapter for calling a REST JSON
API like CouchDB.  The goal is to make something that doesn't need constant
maintenances as additional features are added to the CouchDB API.

For some good documentation of the CouchDB REST API, see:

    http://docs.couchone.com/couchdb-api/

Examples:

>>> var server = new couch.Server('/');
>>> server.put(null, 'mydb');  // Create database 'mydb'
{ok: true}
>>> var database = couch.Database('/mydb');  // One way
>>> var database = server.database('mydb');  // Or another, does same thing
>>> var doc = {foo: 'bar'};
>>> database.save(doc);  // POST to couch, update doc _id & _rev in place
{ok: true, id: '2c370303', rev: '1-7a00dff5'}
>>> doc
{_id: '2c370303', _rev: '1-7a00dff5', foo: 'bar'}
>>> database.post(null, '_compact');  // Compact db 'mydb'
{ok: true}
>>> server.post(null, ['mydb', '_compact']);  // Same as above
{ok: true}

*/

"use strict";

var couch = {};


couch.errors = {
    400: 'BadRequest',
    401: 'Unauthorized',
    403: 'Forbidden',
    404: 'NotFound',
    405: 'MethodNotAllowed',
    406: 'NotAcceptable',
    409: 'Conflict',
    412: 'PreconditionFailed',
    415: 'BadContentType',
    416: 'BadRangeRequest',
    417: 'ExpectationFailed',
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
    path: function(parts, options) {
        /*
        Construct a URL relative to this.url.

        Examples:

        >>> var inst = new couch.CouchBase('/foo/');
        >>> inst.parts();
        '/foo/'
        >>> inst.parts('bar');
        '/foo/bar'
        >>> inst.parts(['bar', 'baz']);
        '/foo/bar/baz'
        >>> inst.parts(['bar', 'baz'], {attachments: true});
        '/foo/bar/baz?attachments=true'

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
            var key;
            for (key in options) {
                keys.push(key);
            }
            keys.sort();
            var query = [];
            keys.forEach(function(key) {
                if (['key', 'startkey', 'endkey'].indexOf(key) > -1) {
                    var value = JSON.stringify(options[key]);
                }
                else {
                    var value = options[key];
                }
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
        if (this.req.status >= 500) {
            throw 'ServerError';
        }
        if (this.req.status >= 400) {
            var error = couch.errors[this.req.status];
            if (error) {
                throw error;
            }
            throw 'ClientError';
        }
        var mime = this.req.getResponseHeader('Content-Type');
        if (mime == 'application/json') {
            return JSON.parse(this.req.responseText);
        }
    },

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
        /*
        Do a GET request.

        Examples:

        var cb = new couch.CouchBase('/');
        cb.get();  # info about server
        cb.get('foo');  # info about db /foo
        cb.get(['foo', 'bar']);  # get doc /foo/bar
        cb.get(['foo', 'bar'], {attachments: true});  # include attachments
        cb.get(['foo', 'bar', 'baz']);  # get attachment /foo/bar/baz
        */
        return this.request('GET', null, parts, options);
    },

    delete: function(parts, options) {
        /*
        Do a DELETE request.

        Examples:

        var cb = new couch.CouchBase('/');
        cb.delete(['foo', 'bar', 'baz'], {rev: '1-blah'});  # delete attachment
        cb.delete(['foo', 'bar'], {rev: '2-flop'});  # delete doc
        cb.delete('foo');  # delete database
        */
        return this.request('DELETE', null, parts, options);
    },
}


// microfiber.Server
couch.Server = function(url, Request) {
    couch.CouchBase.call(this, url, Request);
}
couch.Server.prototype = {
    database: function(name) {
        /*
        Return a new couch.Database whose base url is this.url + name.
        */
        return new couch.Database(this.url + name, this.Request);
    },
}
couch.Server.prototype.__proto__ = couch.CouchBase.prototype;


// microfiber.Database
couch.Database = function(url, Request) {
    couch.CouchBase.call(this, url, Request);
}
couch.Database.prototype = {
    save: function(doc) {
        /*
        Save *doc* to Couch, update *doc* _id and _rev in place.

        Examples:

        >>> var db = new couch.Database('/mydb');
        >>> var doc = {foo: 'bar'};
        >>> db.save(doc);
        {ok: true, id: '2c370303', rev: '1-7a00dff5'}
        >>> doc
        {_id: '2c370303', _rev: '1-7a00dff5', foo: 'bar'}

        */
        var r = this.post(doc);
        doc['_rev'] = r['rev'];
        doc['_id'] = r['id'];
        return r;
    },

    bulksave: function(docs) {
        var rows = this.post({docs: docs, all_or_nothing: true}, '_bulk_docs');
        var i;
        for (i in docs) {
            docs[i]['_rev'] = rows[i]['rev'];
            docs[i]['_id'] = rows[i]['id'];
        }
        return rows;
    },

    view: function(design, view, options) {
        /*
        Shortcut for making a GET request to a view.

        No magic here, just saves you having to type "_design" and "_view" over
        and over.  This:

            Database.view(design, view, options);

        Is just a shortcut for:

            Database.view(['_design', design, '_view', view], options);
        */
        return this.get(['_design', design, '_view', view], options);
    },
}
couch.Database.prototype.__proto__ = couch.CouchBase.prototype;
