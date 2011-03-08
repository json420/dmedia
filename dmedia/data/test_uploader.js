var DummyRequest = new Class({
    initialize: function() {
        this.calls = [];
        var methods = ['open', 'setRequestHeader', 'send'];
        methods.forEach(function(method) {
            this[method] = function() {
                var args = Array.prototype.slice.call(arguments);
                args.unshift(method);
                this.calls.push(args);
            }.bind(this);
        }, this);
    },
});

py.test_b32encode = function() {
    py.data.values.forEach(function(d) {
        py.assertEqual(b32encode(d.src), d.b32);
    });
};

py.test_sha1 = function() {
    py.data.values.forEach(function(d) {
        py.assertEqual(hex_sha1(d.src), d.hex);
        py.assertEqual(b64_sha1(d.src), d.b64);
        py.assertEqual(b32_sha1(d.src), d.b32);
    });
};

py.test_quick_id = function() {
    py.data.values.forEach(function(d) {
        py.assertEqual(quick_id(d.size, d.chunk), d.quick_id);
    });
};

py.test_uploader = function() {
    var url = 'https://example.com/upload/';

    // Test that '/' is appended to URL if it doesn't have it already:
    var u = new Uploader('https://example.com/upload');
    py.assertEqual(u.baseurl, url);

    // Test that default Request is XMLHttpRequest:
    var u = new Uploader(url);
    py.assertTrue(u.Request === XMLHttpRequest);

    // Setup for testing methods, make sure Request can be overridden:
    var u = new Uploader(url, DummyRequest);
    py.assertEqual(u.baseurl, url);
    py.assertTrue(u.Request === DummyRequest);

    // Test Uploader.url():
    py.assertEqual(u.url(), url);
    py.assertEqual(
        u.url('GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN'),
        url + 'GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN'
    );
    py.assertEqual(
        u.url('GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN', 17),
        url + 'GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN/17'
    );
    py.assertEqual(u.url('', 17), url);
    py.assertEqual(u.url(null, 17), url);

    // Test Uploader.hash_leaf():
    py.assertEqual(u.leaves, []);
    py.assertEqual(u.hash_leaf(py.data.leaf, 2), py.data.chash);
    py.assertEqual(u.leaves[2], py.data.chash);

    // Test Uploader.upload_leaf():
    u.quick_id = 'GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN';
    u.upload_leaf(py.data.leaf, py.data.chash, 2);
    py.assertEqual(
        u.request.calls,
        [
            ['open', 'PUT', url + 'GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN/2', true],
            ['setRequestHeader', 'x-dmedia-chash', py.data.chash],
            ['setRequestHeader', 'Content-Type', 'application/octet-stream'],
            ['send', py.data.leaf],
        ]
    );



};
