// Unit tests for browser.js

"use strict";

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
