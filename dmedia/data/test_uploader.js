py.test_b32encode = function() {
    py.data.values.forEach(function(d) {
        py.assertEqual(b32encode(d.src), d.b32);
    });
};

py.test_sha1 = function() {
    py.data.values.forEach(function(d) {
        py.assertEqual(hex_sha1(d.src), d.hex);
        py.assertEqual(b64_sha1(d.src), d.b64);
    });
    py.assertTrue(sha1_vm_test());
};

py.test_uploader = function() {
    var u = new Uploader('https://example.com/upload/');
    py.assertEqual(u.baseurl, 'https://example.com/upload/');
    py.assertEqual(u.url(), 'https://example.com/upload/');

    var u = new Uploader('https://example.com/upload');
    py.assertEqual(u.baseurl, 'https://example.com/upload/');
    py.assertEqual(u.url(), 'https://example.com/upload/');
    py.assertEqual(
        u.url('GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN'),
        'https://example.com/upload/GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN'
    );
    py.assertEqual(
        u.url('GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN', 17),
        'https://example.com/upload/GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN/17'
    );
    py.assertEqual(u.url('', 17), 'https://example.com/upload/');
    py.assertEqual(u.url(null, 17), 'https://example.com/upload/');
};
