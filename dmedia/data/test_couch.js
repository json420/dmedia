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
