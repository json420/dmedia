var perms = {
    parts: [
        [null, ''],
        ['foo', 'foo'],
        [['foo'], 'foo'],
        [['foo', 'bar'], 'foo/bar'],
        ['foo/bar', 'foo/bar'],
    ],

    options: [
        [null, ''],
        [{batch: true}, '?batch=true'],
        [{rev: '1-blah', ok: 17}, '?ok=17&rev=1-blah'],
        [{ok: 17, rev: '1-blah'}, '?ok=17&rev=1-blah'],
    ],
}

var responseObj = {ok: true, id: 'woot', rev: '1-blah'};

function DummyRequest() {
        this.calls = [];
        var methods = ['open', 'setRequestHeader', 'send', 'sendAsBinary'];
        methods.forEach(function(method) {
            var f = function() {
                var args = Array.prototype.slice.call(arguments);
                args.unshift(method);
                this.calls.push(args);
            };
            if (f.bind) {
                var f = f.bind(this);
            }
            this[method] = f;
        }, this);
}
DummyRequest.prototype = {
    getResponseHeader: function(key) {
        this.calls.push(['getResponseHeader', key]);
        if (key == 'Content-Type') {
            return 'application/json';
        }
    },

    responseText: JSON.stringify(responseObj),
}


// couch.CouchBase()
py.test_init = function() {
    // Test default url value
    var inst = new couch.CouchBase();
    py.assertEqual(inst.url, '/');
    var inst = new couch.CouchBase('/foo');
    py.assertEqual(inst.url, '/foo/');
    var inst = new couch.CouchBase('/foo/');
    py.assertEqual(inst.url, '/foo/');
}


// couch.CouchBase.path()
py.test_path = function() {
    var inst = new couch.CouchBase('/foo/');

    py.assertEqual(inst.path(), '/foo/');
    py.assertEqual(inst.path(null), '/foo/');
    py.assertEqual(inst.path([]), '/foo/');
    py.assertEqual(inst.path(''), '/foo/');
    py.assertEqual(inst.path('bar'), '/foo/bar');
    py.assertEqual(inst.path(['bar']), '/foo/bar');
    py.assertEqual(inst.path('bar/baz'), '/foo/bar/baz');
    py.assertEqual(inst.path(['bar', 'baz']), '/foo/bar/baz');

    var options = {rev: '1-3e81', ok: true}
    py.assertEqual(
        inst.path(null, options),
        '/foo/?ok=true&rev=1-3e81'
    );
    py.assertEqual(
        inst.path('bar', options),
        '/foo/bar?ok=true&rev=1-3e81'
    );
    py.assertEqual(
        inst.path(['bar'], options),
        '/foo/bar?ok=true&rev=1-3e81'
    );
    py.assertEqual(
        inst.path('bar/baz', options),
        '/foo/bar/baz?ok=true&rev=1-3e81'
    );
    py.assertEqual(
        inst.path(['bar', 'baz'], options),
        '/foo/bar/baz?ok=true&rev=1-3e81'
    );

    // In different order to make sure keys are sorted
    var options = {ok: true, rev: '1-3e81'}
    py.assertEqual(
        inst.path(null, options),
        '/foo/?ok=true&rev=1-3e81'
    );
    py.assertEqual(
        inst.path('bar', options),
        '/foo/bar?ok=true&rev=1-3e81'
    );
    py.assertEqual(
        inst.path(['bar'], options),
        '/foo/bar?ok=true&rev=1-3e81'
    );
    py.assertEqual(
        inst.path('bar/baz', options),
        '/foo/bar/baz?ok=true&rev=1-3e81'
    );
    py.assertEqual(
        inst.path(['bar', 'baz'], options),
        '/foo/bar/baz?ok=true&rev=1-3e81'
    );
}


// couch.CouchBase.request()
py.test_request = function() {
    var inst = new couch.CouchBase('/', DummyRequest);

    py.assertEqual(
        inst.request('GET', null, 'mydb'),
        responseObj
    );
    py.assertEqual(
        inst.req.calls,
        [
            ['open', 'GET', '/mydb', false],
            ['setRequestHeader', 'Accept', 'application/json'],
            ['send'],
            ['getResponseHeader', 'Content-Type'],
        ]
    );

    inst.request('GET', null, ['mydb', 'mydoc']);
    py.assertEqual(
        inst.req.calls,
        [
            ['open', 'GET', '/mydb/mydoc', false],
            ['setRequestHeader', 'Accept', 'application/json'],
            ['send'],
            ['getResponseHeader', 'Content-Type'],
        ]
    );

    inst.request('GET', null, ['mydb', 'mydoc'], {'rev': '1-foo'});
    py.assertEqual(
        inst.req.calls,
        [
            ['open', 'GET', '/mydb/mydoc?rev=1-foo', false],
            ['setRequestHeader', 'Accept', 'application/json'],
            ['send'],
            ['getResponseHeader', 'Content-Type'],
        ]
    );

    inst.request('POST', null, ['mydb', '_compact']);
    py.assertEqual(
        inst.req.calls,
        [
            ['open', 'POST', '/mydb/_compact', false],
            ['setRequestHeader', 'Accept', 'application/json'],
            ['setRequestHeader', 'Content-Type', 'application/json'],
            ['send'],
            ['getResponseHeader', 'Content-Type'],
        ]
    );

    inst.request('PUT', null, 'mydb');
    py.assertEqual(
        inst.req.calls,
        [
            ['open', 'PUT', '/mydb', false],
            ['setRequestHeader', 'Accept', 'application/json'],
            ['setRequestHeader', 'Content-Type', 'application/json'],
            ['send'],
            ['getResponseHeader', 'Content-Type'],
        ]
    );

    var doc = {'foo': 'bar', 'ok': 17};
    inst.request('PUT', doc, ['mydb', 'mydoc']);
    py.assertEqual(
        inst.req.calls,
        [
            ['open', 'PUT', '/mydb/mydoc', false],
            ['setRequestHeader', 'Accept', 'application/json'],
            ['setRequestHeader', 'Content-Type', 'application/json'],
            ['send', JSON.stringify(doc)],
            ['getResponseHeader', 'Content-Type'],
        ]
    );
}


// couch.CouchBase.post()
py.test_post = function() {
    var doc = {'foo': 'bar', 'ok': 17};
    var server = new couch.Server('/', DummyRequest);
    var database = new couch.Database('/aye/', DummyRequest);

    for (i in perms.parts) {
        var p = perms.parts[i];
        for (j in perms.options) {
            var o = perms.options[j];

            //////////////
            // Test server
            var url = ('/' + p[1] + o[1]);

            // Test with obj=null
            py.assertEqual(
                server.post(null, p[0], o[0]),
                responseObj
            );
            py.assertEqual(
                server.req.calls,
                [
                    ['open', 'POST', url, false],
                    ['setRequestHeader', 'Accept', 'application/json'],
                    ['setRequestHeader', 'Content-Type', 'application/json'],
                    ['send'],
                    ['getResponseHeader', 'Content-Type'],
                ]
            );

            // Test with obj=doc
            py.assertEqual(
                server.post(doc, p[0], o[0]),
                responseObj
            );
            py.assertEqual(
                server.req.calls,
                [
                    ['open', 'POST', url, false],
                    ['setRequestHeader', 'Accept', 'application/json'],
                    ['setRequestHeader', 'Content-Type', 'application/json'],
                    ['send', JSON.stringify(doc)],
                    ['getResponseHeader', 'Content-Type'],
                ]
            );

            ////////////////
            // Test database
            var url = ('/aye/' + p[1] + o[1]);

            // Test with obj=null
            py.assertEqual(
                database.post(null, p[0], o[0]),
                responseObj
            );
            py.assertEqual(
                database.req.calls,
                [
                    ['open', 'POST', url, false],
                    ['setRequestHeader', 'Accept', 'application/json'],
                    ['setRequestHeader', 'Content-Type', 'application/json'],
                    ['send'],
                    ['getResponseHeader', 'Content-Type'],
                ]
            );

            // Test with obj=doc
            py.assertEqual(
                database.post(doc, p[0], o[0]),
                responseObj
            );
            py.assertEqual(
                database.req.calls,
                [
                    ['open', 'POST', url, false],
                    ['setRequestHeader', 'Accept', 'application/json'],
                    ['setRequestHeader', 'Content-Type', 'application/json'],
                    ['send', JSON.stringify(doc)],
                    ['getResponseHeader', 'Content-Type'],
                ]
            );

        }
    }
    py.assertEqual(i * 1, perms.parts.length - 1);
    py.assertEqual(j * 1, perms.options.length - 1);
}


// couch.CouchBase.put()
py.test_put = function() {
    var doc = {'foo': 'bar', 'ok': 17};
    var server = new couch.Server('/', DummyRequest);
    var database = new couch.Database('/aye/', DummyRequest);

    for (i in perms.parts) {
        var p = perms.parts[i];
        for (j in perms.options) {
            var o = perms.options[j];

            //////////////
            // Test server
            var url = ('/' + p[1] + o[1]);

            // Test with obj=null
            py.assertEqual(
                server.put(null, p[0], o[0]),
                responseObj
            );
            py.assertEqual(
                server.req.calls,
                [
                    ['open', 'PUT', url, false],
                    ['setRequestHeader', 'Accept', 'application/json'],
                    ['setRequestHeader', 'Content-Type', 'application/json'],
                    ['send'],
                    ['getResponseHeader', 'Content-Type'],
                ]
            );

            // Test with obj=doc
            py.assertEqual(
                server.put(doc, p[0], o[0]),
                responseObj
            );
            py.assertEqual(
                server.req.calls,
                [
                    ['open', 'PUT', url, false],
                    ['setRequestHeader', 'Accept', 'application/json'],
                    ['setRequestHeader', 'Content-Type', 'application/json'],
                    ['send', JSON.stringify(doc)],
                    ['getResponseHeader', 'Content-Type'],
                ]
            );

            ////////////////
            // Test database
            var url = ('/aye/' + p[1] + o[1]);

            // Test with obj=null
            py.assertEqual(
                database.put(null, p[0], o[0]),
                responseObj
            );
            py.assertEqual(
                database.req.calls,
                [
                    ['open', 'PUT', url, false],
                    ['setRequestHeader', 'Accept', 'application/json'],
                    ['setRequestHeader', 'Content-Type', 'application/json'],
                    ['send'],
                    ['getResponseHeader', 'Content-Type'],
                ]
            );

            // Test with obj=doc
            py.assertEqual(
                database.put(doc, p[0], o[0]),
                responseObj
            );
            py.assertEqual(
                database.req.calls,
                [
                    ['open', 'PUT', url, false],
                    ['setRequestHeader', 'Accept', 'application/json'],
                    ['setRequestHeader', 'Content-Type', 'application/json'],
                    ['send', JSON.stringify(doc)],
                    ['getResponseHeader', 'Content-Type'],
                ]
            );

        }
    }
    py.assertEqual(i * 1, perms.parts.length - 1);
    py.assertEqual(j * 1, perms.options.length - 1);
}


// couch.CouchBase.get()
py.test_get = function() {
    var server = new couch.Server('/', DummyRequest);
    var database = new couch.Database('/aye/', DummyRequest);

    for (i in perms.parts) {
        var p = perms.parts[i];
        for (j in perms.options) {
            var o = perms.options[j];

            //////////////
            // Test server
            var url = ('/' + p[1] + o[1]);
            py.assertEqual(
                server.get(p[0], o[0]),
                responseObj
            );
            py.assertEqual(
                server.req.calls,
                [
                    ['open', 'GET', url, false],
                    ['setRequestHeader', 'Accept', 'application/json'],
                    ['send'],
                    ['getResponseHeader', 'Content-Type'],
                ]
            );

            ////////////////
            // Test database
            var url = ('/aye/' + p[1] + o[1]);
            py.assertEqual(
                database.get(p[0], o[0]),
                responseObj
            );
            py.assertEqual(
                database.req.calls,
                [
                    ['open', 'GET', url, false],
                    ['setRequestHeader', 'Accept', 'application/json'],
                    ['send'],
                    ['getResponseHeader', 'Content-Type'],
                ]
            );

        }
    }
    py.assertEqual(i * 1, perms.parts.length - 1);
    py.assertEqual(j * 1, perms.options.length - 1);
}


// couch.CouchBase.delete()
py.test_delete = function() {
    var server = new couch.Server('/', DummyRequest);
    var database = new couch.Database('/aye/', DummyRequest);

    for (i in perms.parts) {
        var p = perms.parts[i];
        for (j in perms.options) {
            var o = perms.options[j];

            //////////////
            // Test server
            var url = ('/' + p[1] + o[1]);
            py.assertEqual(
                server.delete(p[0], o[0]),
                responseObj
            );
            py.assertEqual(
                server.req.calls,
                [
                    ['open', 'DELETE', url, false],
                    ['setRequestHeader', 'Accept', 'application/json'],
                    ['send'],
                    ['getResponseHeader', 'Content-Type'],
                ]
            );

            ////////////////
            // Test database
            var url = ('/aye/' + p[1] + o[1]);
            py.assertEqual(
                database.delete(p[0], o[0]),
                responseObj
            );
            py.assertEqual(
                database.req.calls,
                [
                    ['open', 'DELETE', url, false],
                    ['setRequestHeader', 'Accept', 'application/json'],
                    ['send'],
                    ['getResponseHeader', 'Content-Type'],
                ]
            );

        }
    }
    py.assertEqual(i * 1, perms.parts.length - 1);
    py.assertEqual(j * 1, perms.options.length - 1);
}


// couch.Server.database()
py.test_database = function() {
    var s = new couch.Server('/', DummyRequest);
    var db = s.database('mydb');
    py.assertTrue(db instanceof couch.Database);
    py.assertTrue(db instanceof couch.CouchBase);
    py.assertEqual(db.url, '/mydb/');
    py.assertTrue(db.Request == DummyRequest);
}
