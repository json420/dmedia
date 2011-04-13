"use strict";

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

py.test_dollar_replace = function() {
    // By id
    var oldEl = $('example');
    py.assertEqual(oldEl.tagName, 'DIV');
    var newEl = $el('p', {'id': 'foo'});
    py.assertTrue($replace('example', newEl) == oldEl);
    py.assertIsNone(oldEl.parentNode);
    py.assertEqual(newEl.parentNode.tagName, 'BODY');
    py.assertTrue($('foo') == newEl);

    // By el
    var oldEl = $('foo');
    py.assertEqual(oldEl.tagName, 'P');
    var newEl = $el('table', {'id': 'bar'});
    py.assertTrue($replace(oldEl, newEl) == oldEl);
    py.assertIsNone(oldEl.parentNode);
    py.assertEqual(newEl.parentNode.tagName, 'BODY');
    py.assertTrue($('bar') == newEl);
}

py.test_dollar_hide = function() {
    // By id
    var el = $('example');
    py.assertFalse(el.classList.contains('hide'));
    $hide('example');
    py.assertTrue(el.classList.contains('hide'));
    $hide('example');
    py.assertTrue(el.classList.contains('hide'));

    // By element
    var el = $el('button');
    py.assertFalse(el.classList.contains('hide'));
    $hide(el);
    py.assertTrue(el.classList.contains('hide'));
    $hide(el);
    py.assertTrue(el.classList.contains('hide'));
}

py.test_dollar_show = function() {
    // By id
    var el = $('example');
    el.classList.add('hide');
    py.assertTrue(el.classList.contains('hide'));
    $show('example');
    py.assertFalse(el.classList.contains('hide'));
    $show('example');
    py.assertFalse(el.classList.contains('hide'));

    // By element
    var el = $el('button', {'class': 'hide'});
    py.assertTrue(el.classList.contains('hide'));
    $show(el);
    py.assertFalse(el.classList.contains('hide'));
    $show(el);
    py.assertFalse(el.classList.contains('hide'));
}

py.test_minsec = function() {
    py.assertIsNone(minsec('hello'));
    py.assertEqual(minsec(0), '0:00');
    py.assertEqual(minsec(3), '0:03');
    py.assertEqual(minsec(17), '0:17');
    py.assertEqual(minsec(69), '1:09');
}

py.test_todata = function() {
    py.assertIsNone(todata(17));
    py.assertIsNone(todata({}));
    py.assertIsNone(todata({content_type: true, data: 'foo'}));
    py.assertIsNone(todata({content_type: 'image/jpeg', data: 17}));
    py.assertEqual(
        todata({content_type: 'image/jpeg', data: 'foo'}),
        'data:image/jpeg;base64,foo'
    );
}
