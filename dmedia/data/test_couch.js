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


py.test_stuff = function() {
    py.assertEqual(
        couch.stuff(),
        'Woot!'
    );
}

py.test_junk = function() {
    py.assertEqual(couch.awesome('Akshat'), 'Akshat is awesome!');
    py.assertEqual(couch.awesome('CouchDB'), 'CouchDB is awesome!');
}

py.test_init = function() {
    // Test default url value
    var inst = new couch.CouchBase();
    py.assertEqual(inst.url, '/');
    var inst = new couch.CouchBase('/foo');
    py.assertEqual(inst.url, '/foo/');
    var inst = new couch.CouchBase('/foo/');
    py.assertEqual(inst.url, '/foo/');
}

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

py.test_request = function() {
    var inst = new couch.CouchBase('/', DummyRequest);

    inst.request('GET', null, 'mydb');
    py.assertEqual(
        inst.req.calls,
        [
            ['open', 'GET', '/mydb', false],
            ['setRequestHeader', 'Accept', 'application/json'],
            ['send'],
        ]
    );

    inst.request('GET', null, ['mydb', 'mydoc']);
    py.assertEqual(
        inst.req.calls,
        [
            ['open', 'GET', '/mydb/mydoc', false],
            ['setRequestHeader', 'Accept', 'application/json'],
            ['send'],
        ]
    );

    inst.request('GET', null, ['mydb', 'mydoc'], {'rev': '1-foo'});
    py.assertEqual(
        inst.req.calls,
        [
            ['open', 'GET', '/mydb/mydoc?rev=1-foo', false],
            ['setRequestHeader', 'Accept', 'application/json'],
            ['send'],
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
        ]
    );

}
