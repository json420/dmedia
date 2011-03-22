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
