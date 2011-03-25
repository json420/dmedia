py.test_dollar = function() {
    // Retrieve by ID:
    var el = $('example');
    py.assertTrue(el instanceof Element);
    py.assertEqual(el.tagName, 'DIV');
    py.assertEqual(el.id, 'example');

    // Make sure if you call with an Element, it's just reterned unchanged:
    var em = $(el);
    py.assertTrue(em instanceof Element);
    py.assertEqual(em.tagName, 'DIV');
    py.assertEqual(em.id, 'example');
    py.assertTrue(em == el);
}
