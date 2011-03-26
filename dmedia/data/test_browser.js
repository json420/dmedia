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


py.test_dollar_el = function() {
    // Retrieve by ID:
    var el = $el('button');
    py.assertTrue(el instanceof Element);
    py.assertEqual(el.tagName, 'BUTTON');

    var el = $el('img', {'class': 'foo', 'id': 'bar', 'src': 'baz.jpg'});
    py.assertTrue(el instanceof Element);
    py.assertEqual(el.tagName, 'IMG');
    py.assertEqual(el.className, 'foo');
    py.assertEqual(el.id, 'bar');
    py.assertEqual(el.getAttribute('src'), 'baz.jpg');
}


py.test_init = function() {
    // Retrieve by ID:
    var db = new couch.Database('/couch/');
    var b = new Browser('example', db);
    py.assertTrue(b.el instanceof Element);
    py.assertEqual(b.el.tagName, 'DIV');
    py.assertEqual(b.el.id, 'example');
    py.assertTrue(b.db == db);
    var el = b.el;

    // Make sure if you call with an Element, it's just reterned unchanged:
    var b = new Browser(el, db);
    py.assertTrue(b.el instanceof Element);
    py.assertEqual(b.el.tagName, 'DIV');
    py.assertEqual(b.el.id, 'example');
    py.assertTrue(b.el == el);
    py.assertTrue(b.db == db);
}
