// Unit tests for couch.js

"use strict";

var py = py || {};

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


function dummy_request(status, responseObj) {
    /*
    Factory to create a new DummyRequest class.

    The DummyRequest is used in place of XMLHttpRequest for unit testing.

    Examples:

    var DummyRequest = dummy_request(201, {'ok': true});
    var DummyRequest = dummy_request(404);
    */

    function Dummy() {
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
    Dummy.prototype = {
        getResponseHeader: function(key) {
            this.calls.push(['getResponseHeader', key]);
            if (key == 'Content-Type') {
                return 'application/json';
            }
        },

        responseText: JSON.stringify(responseObj),

        status: status,
    }

    return Dummy;
}

var responseObj = {ok: true, id: 'woot', rev: '1-blah'};
var DummyRequest = dummy_request(201, responseObj);


// couch.CouchRequst:
py.TestCouchRequest = {

    // couch.CouchRequest.request()
    test_request: function() {
        var doc = {'foo': 'bar', 'ok': 17};
        var url = '/foo/bar/baz?ok=true&rev=1-3e81';

        // PUT
        var r = new couch.CouchRequest(DummyRequest);
        py.assertEqual(r.req.calls, []);
        py.assertIsNone(r.request('PUT', doc, url));
        py.assertEqual(
            r.req.calls,
            [
                ['open', 'PUT', url, false],
                ['setRequestHeader', 'Accept', 'application/json'],
                ['setRequestHeader', 'Content-Type', 'application/json'],
                ['send', JSON.stringify(doc)],
            ]
        );
        var r = new couch.CouchRequest(DummyRequest);
        py.assertEqual(r.req.calls, []);
        py.assertIsNone(r.request('PUT', null, url));
        py.assertEqual(
            r.req.calls,
            [
                ['open', 'PUT', url, false],
                ['setRequestHeader', 'Accept', 'application/json'],
                ['setRequestHeader', 'Content-Type', 'application/json'],
                ['send'],
            ]
        );

        // POST
        var r = new couch.CouchRequest(DummyRequest);
        py.assertEqual(r.req.calls, []);
        py.assertIsNone(r.request('POST', doc, url));
        py.assertEqual(
            r.req.calls,
            [
                ['open', 'POST', url, false],
                ['setRequestHeader', 'Accept', 'application/json'],
                ['setRequestHeader', 'Content-Type', 'application/json'],
                ['send', JSON.stringify(doc)],
            ]
        );
        var r = new couch.CouchRequest(DummyRequest);
        py.assertEqual(r.req.calls, []);
        py.assertIsNone(r.request('POST', null, url));
        py.assertEqual(
            r.req.calls,
            [
                ['open', 'POST', url, false],
                ['setRequestHeader', 'Accept', 'application/json'],
                ['setRequestHeader', 'Content-Type', 'application/json'],
                ['send'],
            ]
        );

        // GET
        var r = new couch.CouchRequest(DummyRequest);
        py.assertEqual(r.req.calls, []);
        py.assertIsNone(r.request('GET', null, url));
        py.assertEqual(
            r.req.calls,
            [
                ['open', 'GET', url, false],
                ['setRequestHeader', 'Accept', 'application/json'],
                ['send'],
            ]
        );

        // DELETE
        var r = new couch.CouchRequest(DummyRequest);
        py.assertEqual(r.req.calls, []);
        py.assertIsNone(r.request('DELETE', null, url));
        py.assertEqual(
            r.req.calls,
            [
                ['open', 'DELETE', url, false],
                ['setRequestHeader', 'Accept', 'application/json'],
                ['send'],
            ]
        );

    },

    // couch.CouchRequest.request()
    test_async_request: function() {
        var doc = {'foo': 'bar', 'ok': 17};
        var url = '/foo/bar/baz?ok=true&rev=1-3e81';

        var callback = function(req) {
            req.read();
        }

        // PUT doc
        var r = new couch.CouchRequest(DummyRequest);
        py.assertIsNone(r.callback);
        py.assertIsNone(r.req.onreadystatechange);
        py.assertEqual(r.req.calls, []);
        py.assertIsNone(r.async_request(callback, 'PUT', doc, url));
        py.assertTrue(r.callback == callback);
        py.assertTrue(r.req.onreadystatechange instanceof Function);
        py.assertEqual(
            r.req.calls,
            [
                ['open', 'PUT', url, true],
                ['setRequestHeader', 'Accept', 'application/json'],
                ['setRequestHeader', 'Content-Type', 'application/json'],
                ['send', JSON.stringify(doc)],
            ]
        );

        // PUT null
        var r = new couch.CouchRequest(DummyRequest);
        py.assertIsNone(r.callback);
        py.assertIsNone(r.req.onreadystatechange);
        py.assertEqual(r.req.calls, []);
        py.assertIsNone(r.async_request(callback, 'PUT', null, url));
        py.assertTrue(r.callback == callback);
        py.assertTrue(r.req.onreadystatechange instanceof Function);
        py.assertEqual(
            r.req.calls,
            [
                ['open', 'PUT', url, true],
                ['setRequestHeader', 'Accept', 'application/json'],
                ['setRequestHeader', 'Content-Type', 'application/json'],
                ['send'],
            ]
        );

        // POST doc
        var r = new couch.CouchRequest(DummyRequest);
        py.assertIsNone(r.callback);
        py.assertIsNone(r.req.onreadystatechange);
        py.assertEqual(r.req.calls, []);
        py.assertIsNone(r.async_request(callback, 'POST', doc, url));
        py.assertTrue(r.callback == callback);
        py.assertTrue(r.req.onreadystatechange instanceof Function);
        py.assertEqual(
            r.req.calls,
            [
                ['open', 'POST', url, true],
                ['setRequestHeader', 'Accept', 'application/json'],
                ['setRequestHeader', 'Content-Type', 'application/json'],
                ['send', JSON.stringify(doc)],
            ]
        );

        // POST null
        var r = new couch.CouchRequest(DummyRequest);
        py.assertIsNone(r.callback);
        py.assertIsNone(r.req.onreadystatechange);
        py.assertEqual(r.req.calls, []);
        py.assertIsNone(r.async_request(callback, 'POST', null, url));
        py.assertTrue(r.callback == callback);
        py.assertTrue(r.req.onreadystatechange instanceof Function);
        py.assertEqual(
            r.req.calls,
            [
                ['open', 'POST', url, true],
                ['setRequestHeader', 'Accept', 'application/json'],
                ['setRequestHeader', 'Content-Type', 'application/json'],
                ['send'],
            ]
        );

        // GET
        var r = new couch.CouchRequest(DummyRequest);
        py.assertIsNone(r.callback);
        py.assertIsNone(r.req.onreadystatechange);
        py.assertEqual(r.req.calls, []);
        py.assertIsNone(r.async_request(callback, 'GET', null, url));
        py.assertTrue(r.callback == callback);
        py.assertTrue(r.req.onreadystatechange instanceof Function);
        py.assertEqual(
            r.req.calls,
            [
                ['open', 'GET', url, true],
                ['setRequestHeader', 'Accept', 'application/json'],
                ['send'],
            ]
        );

        // DELETE
        var r = new couch.CouchRequest(DummyRequest);
        py.assertIsNone(r.callback);
        py.assertIsNone(r.req.onreadystatechange);
        py.assertEqual(r.req.calls, []);
        py.assertIsNone(r.async_request(callback, 'DELETE', null, url));
        py.assertTrue(r.callback == callback);
        py.assertTrue(r.req.onreadystatechange instanceof Function);
        py.assertEqual(
            r.req.calls,
            [
                ['open', 'DELETE', url, true],
                ['setRequestHeader', 'Accept', 'application/json'],
                ['send'],
            ]
        );

    },

}


// couch.CouchBase:
py.TestCouchBase = {

    test_init: function() {
        // Test default url value
        var inst = new couch.CouchBase();
        py.assertEqual(inst.url, '/');
        var inst = new couch.CouchBase('/foo');
        py.assertEqual(inst.url, '/foo/');
        var inst = new couch.CouchBase('/foo/');
        py.assertEqual(inst.url, '/foo/');
    },

    // couch.CouchBase.path()
    test_path: function() {
        /////////////////////////////////
        // Test against a Server at /foo/
        var inst = new couch.Server('/foo/');
        py.assertEqual(inst.url, '/foo/');
        py.assertEqual(inst.basepath, '/foo/');

        py.assertEqual(inst.path(), '/foo/');
        py.assertEqual(inst.path(null), '/foo/');
        py.assertEqual(inst.path([]), '/foo/');
        py.assertEqual(inst.path(''), '/foo/');
        py.assertEqual(inst.path('bar'), '/foo/bar');
        py.assertEqual(inst.path(['bar']), '/foo/bar');
        py.assertEqual(inst.path('bar/baz'), '/foo/bar/baz');
        py.assertEqual(inst.path(['bar', 'baz']), '/foo/bar/baz');

        // Test with empty options
        py.assertEqual(
            inst.path(null, {}),
            '/foo/'
        );
        py.assertEqual(
            inst.path('bar', {}),
            '/foo/bar'
        );
        py.assertEqual(
            inst.path(['bar'], {}),
            '/foo/bar'
        );
        py.assertEqual(
            inst.path(['bar', 'baz'], {}),
            '/foo/bar/baz'
        );

        // Test with options
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

        // Test with options in different order to make sure keys are sorted
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

        var options = {'key': 'foo', 'startkey': 'bar', 'endkey': 'baz'};
        py.assertEqual(
            inst.path(['bar', 'baz'], options),
            '/foo/bar/baz?endkey=%22baz%22&key=%22foo%22&startkey=%22bar%22'
        );


        /////////////////////////////////////
        // Test against a "Database" at /foo/
        var inst = new couch.Database('foo', '/');
        py.assertEqual(inst.url, '/');
        py.assertEqual(inst.basepath, '/foo/');
        py.assertEqual(inst.name, 'foo');

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

        var options = {'key': 'foo', 'startkey': 'bar', 'endkey': 'baz'};
        py.assertEqual(
            inst.path(['bar', 'baz'], options),
            '/foo/bar/baz?endkey=%22baz%22&key=%22foo%22&startkey=%22bar%22'
        );

    },


    // couch.CouchBase.request()
    test_request: function() {
        var inst = new couch.CouchBase('/', DummyRequest);

        py.assertEqual(
            inst.request('GET', null, 'mydb'),
            responseObj
        );
        py.assertEqual(
            inst.req.req.calls,
            [
                ['open', 'GET', '/mydb', false],
                ['setRequestHeader', 'Accept', 'application/json'],
                ['send'],
                ['getResponseHeader', 'Content-Type'],
            ]
        );

        inst.request('GET', null, ['mydb', 'mydoc']);
        py.assertEqual(
            inst.req.req.calls,
            [
                ['open', 'GET', '/mydb/mydoc', false],
                ['setRequestHeader', 'Accept', 'application/json'],
                ['send'],
                ['getResponseHeader', 'Content-Type'],
            ]
        );

        inst.request('GET', null, ['mydb', 'mydoc'], {'rev': '1-foo'});
        py.assertEqual(
            inst.req.req.calls,
            [
                ['open', 'GET', '/mydb/mydoc?rev=1-foo', false],
                ['setRequestHeader', 'Accept', 'application/json'],
                ['send'],
                ['getResponseHeader', 'Content-Type'],
            ]
        );

        inst.request('POST', null, ['mydb', '_compact']);
        py.assertEqual(
            inst.req.req.calls,
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
            inst.req.req.calls,
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
            inst.req.req.calls,
            [
                ['open', 'PUT', '/mydb/mydoc', false],
                ['setRequestHeader', 'Accept', 'application/json'],
                ['setRequestHeader', 'Content-Type', 'application/json'],
                ['send', JSON.stringify(doc)],
                ['getResponseHeader', 'Content-Type'],
            ]
        );

        //////////////////////////
        // Test exception throwing
        var inst = new couch.CouchBase('/', dummy_request(null));
        try {
            inst.request('GET');
            py.assertTrue(false);
        }
        catch (e) {
            py.assertEqual(e, 'RequestError');
        }

        var inst = new couch.CouchBase('/', dummy_request(0));
        try {
            inst.request('GET');
            py.assertTrue(false);
        }
        catch (e) {
            py.assertEqual(e, 'RequestError');
        }

        var inst = new couch.CouchBase('/', dummy_request(400));
        try {
            inst.request('GET');
            py.assertTrue(false);
        }
        catch (e) {
            py.assertEqual(e, 'BadRequest');
        }

        var inst = new couch.CouchBase('/', dummy_request(401));
        try {
            inst.request('GET');
            py.assertTrue(false);
        }
        catch (e) {
            py.assertEqual(e, 'Unauthorized');
        }

        var inst = new couch.CouchBase('/', dummy_request(402));
        try {
            inst.request('GET');
            py.assertTrue(false);
        }
        catch (e) {  // No specific 402 error, so generic 4xx ClientError is thrown
            py.assertEqual(e, 'ClientError');
        }

        var inst = new couch.CouchBase('/', dummy_request(403));
        try {
            inst.request('GET');
            py.assertTrue(false);
        }
        catch (e) {
            py.assertEqual(e, 'Forbidden');
        }

        var inst = new couch.CouchBase('/', dummy_request(404));
        try {
            inst.request('GET');
            py.assertTrue(false);
        }
        catch (e) {
            py.assertEqual(e, 'NotFound');
        }

        var inst = new couch.CouchBase('/', dummy_request(405));
        try {
            inst.request('GET');
            py.assertTrue(false);
        }
        catch (e) {
            py.assertEqual(e, 'MethodNotAllowed');
        }

        var inst = new couch.CouchBase('/', dummy_request(406));
        try {
            inst.request('GET');
            py.assertTrue(false);
        }
        catch (e) {
            py.assertEqual(e, 'NotAcceptable');
        }

        var inst = new couch.CouchBase('/', dummy_request(409));
        try {
            inst.request('GET');
            py.assertTrue(false);
        }
        catch (e) {
            py.assertEqual(e, 'Conflict');
        }

        var inst = new couch.CouchBase('/', dummy_request(412));
        try {
            inst.request('GET');
            py.assertTrue(false);
        }
        catch (e) {
            py.assertEqual(e, 'PreconditionFailed');
        }

        var inst = new couch.CouchBase('/', dummy_request(415));
        try {
            inst.request('GET');
            py.assertTrue(false);
        }
        catch (e) {
            py.assertEqual(e, 'BadContentType');
        }

        var inst = new couch.CouchBase('/', dummy_request(416));
        try {
            inst.request('GET');
            py.assertTrue(false);
        }
        catch (e) {
            py.assertEqual(e, 'BadRangeRequest');
        }

        var inst = new couch.CouchBase('/', dummy_request(417));
        try {
            inst.request('GET');
            py.assertTrue(false);
        }
        catch (e) {
            py.assertEqual(e, 'ExpectationFailed');
        }

        var inst = new couch.CouchBase('/', dummy_request(499));
        try {
            inst.request('GET');
            py.assertTrue(false);
        }
        catch (e) {  // No specific 499 error, so generic 4xx ClientError is thrown
            py.assertEqual(e, 'ClientError');
        }

        // Any 5xx status is thrown as generic ServerError
        var inst = new couch.CouchBase('/', dummy_request(500));
        try {
            inst.request('GET');
            py.assertTrue(false);
        }
        catch (e) {
            py.assertEqual(e, 'ServerError');
        }

        var inst = new couch.CouchBase('/', dummy_request(569));
        try {
            inst.request('GET');
            py.assertTrue(false);
        }
        catch (e) {
            py.assertEqual(e, 'ServerError');
        }
    },


    // couch.CouchBase.post()
    test_post: function() {
        var doc = {'foo': 'bar', 'ok': 17};
        var server = new couch.Server('/', DummyRequest);
        var database = new couch.Database('aye', '/', DummyRequest);

        var i, j;
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
                    server.req.req.calls,
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
                    server.req.req.calls,
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
                    database.req.req.calls,
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
                    database.req.req.calls,
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
    },

    // couch.CouchBase.put()
    test_put: function() {
        var doc = {'foo': 'bar', 'ok': 17};
        var server = new couch.Server('/', DummyRequest);
        var database = new couch.Database('aye', '/', DummyRequest);

        var i, j;
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
                    server.req.req.calls,
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
                    server.req.req.calls,
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
                    database.req.req.calls,
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
                    database.req.req.calls,
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
    },


    // couch.CouchBase.get()
    test_get: function() {
        var server = new couch.Server('/', DummyRequest);
        var database = new couch.Database('aye', '/', DummyRequest);

        var i, j;
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
                    server.req.req.calls,
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
                    database.req.req.calls,
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
    },


    // couch.CouchBase.delete()
    test_delete: function() {
        var server = new couch.Server('/', DummyRequest);
        var database = new couch.Database('aye', '/', DummyRequest);

        var i, j;
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
                    server.req.req.calls,
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
                    database.req.req.calls,
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
    },
}


// couch.Server
py.TestServer = {

    // couch.Server.database()
    test_database: function() {
        var s = new couch.Server('/', DummyRequest);
        var db = s.database('mydb');
        py.assertTrue(db instanceof couch.Database);
        py.assertTrue(db instanceof couch.CouchBase);
        py.assertEqual(db.url, '/');
        py.assertEqual(db.basepath, '/mydb/');
        py.assertEqual(db.name, 'mydb');
        py.assertTrue(db.Request == DummyRequest);
    },
}


// couch.Database
py.TestDatabase = {

    // couch.Database.save()
    test_save: function() {
        var db = new couch.Database('mydb', '/', DummyRequest);
        var doc = {'foo': 'bar'};
        var data = JSON.stringify(doc);

        py.assertEqual(
            db.save(doc),
            {'ok': true, 'id': 'woot', 'rev': '1-blah'}
        );
        py.assertEqual(
            db.req.req.calls,
            [
                ['open', 'POST', '/mydb/', false],
                ['setRequestHeader', 'Accept', 'application/json'],
                ['setRequestHeader', 'Content-Type', 'application/json'],
                ['send', data],
                ['getResponseHeader', 'Content-Type'],
            ]
        );
        py.assertEqual(
            doc,
            {'_id': 'woot', '_rev': '1-blah', 'foo': 'bar'}
        );
    },

    // couch.Database.bulksave()
    test_bulksave: function() {
        var responseObj = [
            {'id': 'foo', 'rev': '1-blah'},
            {'id': 'bar', 'rev': '2-lala'},
            {'id': 'baz', 'rev': '1-junk'},
        ];
        var docs = [
            {'hello': 'world'},
            {'whatup': 'dog'},
            {'hello': 'naughty nurse'},
        ];
        var data = JSON.stringify({'docs': docs, 'all_or_nothing': true});

        var db = new couch.Database('mydb', '/', dummy_request(201, responseObj));
        py.assertEqual(
            db.bulksave(docs),
            responseObj
        );
        py.assertEqual(
            db.req.req.calls,
            [
                ['open', 'POST', '/mydb/_bulk_docs', false],
                ['setRequestHeader', 'Accept', 'application/json'],
                ['setRequestHeader', 'Content-Type', 'application/json'],
                ['send', data],
                ['getResponseHeader', 'Content-Type'],
            ]
        );
        py.assertEqual(
            docs,
            [
                {'_id': 'foo', '_rev': '1-blah', 'hello': 'world'},
                {'_id': 'bar', '_rev': '2-lala', 'whatup': 'dog'},
                {'_id': 'baz', '_rev': '1-junk', 'hello': 'naughty nurse'},
            ]
        );
    },

    // couch.Database.view()
    test_view: function() {
        var responseObj = {'rows': [{'value': 80, 'key': null}]};
        var db = new couch.Database('dmedia', '/', dummy_request(201, responseObj));

        py.assertEqual(
            db.view('file', 'ext'),
            responseObj
        );
        py.assertEqual(
            db.req.req.calls,
            [
                ['open', 'GET', '/dmedia/_design/file/_view/ext', false],
                ['setRequestHeader', 'Accept', 'application/json'],
                ['send'],
                ['getResponseHeader', 'Content-Type'],
            ]
        );
    },
}
