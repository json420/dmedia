/* Example test in js.py module docstring */
function bar() {
    return 'My JS unit testing is awesome';
}

py.test_bar = function() {
    py.assertEqual(bar(), 'My JS unit testing is awesome');
    py.assertNotEqual(bar(), 'My JS unit testing is lame');
};


/* Run a selftest on the tester */
py.test_self =  function() {
    py.assertTrue(true);
    py.assertFalse(false);

    py.assertEqual('foo', 'foo');
    py.assertNotEqual('foo', 'bar');
    py.assertAlmostEqual(1.2, 1.2);
    py.assertNotAlmostEqual(1.2, 1.3);

    py.assertGreater(3, 2);
    py.assertGreaterEqual(3, 3);
    py.assertLess(2, 3);
    py.assertLessEqual(2, 2);

    py.assertIn('bar', ['foo', 'bar', 'baz']);
    py.assertNotIn('car', ['foo', 'bar', 'baz']);
    py.assertItemsEqual(['foo', 'bar', 'baz'], ['baz', 'foo', 'bar']);

    py.assertIsNone(null);
    py.assertIsNotNone(false);

    py.assertRegexpMatches('foo', '^[a-z]+$');
    py.assertNotRegexpMatches('bAr', '^[a-z]+$');
};
