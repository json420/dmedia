"use strict";

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
