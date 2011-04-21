function DummyRequest() {
        this.calls = [];
        var methods = ['open', 'setRequestHeader', 'send', 'sendAsBinary'];
        methods.forEach(function(method) {
            var f = function() {
                var args = Array.prototype.slice.call(arguments);
                args.unshift(method);
                this.calls.push(args);
            };
            if (f.bind) {
                var f = f.bind(this);
            }
            this[method] = f;
        }, this);
}
DummyRequest.prototype = {
    onreadystatechange: null,
}


var DummyFile = {
    'size': 20202333,
    'name': 'MVI_5751.MOV',
    'type': 'video/quicktime',
}


py.TestUploader = {

    test_b32encode: function() {
        py.data.values.forEach(function(d) {
            py.assertEqual(b32encode(d.src), d.b32);
        });
    },

    test_sha1: function() {
        py.data.values.forEach(function(d) {
            py.assertEqual(hex_sha1(d.src), d.hex);
            py.assertEqual(b32_sha1(d.src), d.b32);
        });
    },

    test_quick_id: function() {
        py.data.values.forEach(function(d) {
            py.assertEqual(quick_id(d.size, d.chunk), d.quick_id);
        });
    },

    test_uploader: function() {
        var d = new DummyRequest();
        d.open('foo', 'bar');
        py.assertEqual(d.calls, [['open', 'foo', 'bar']]);

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
        u.leaves = [];
        py.assertEqual(u.hash_leaf(py.data.leaf, 2), py.data.chash);
        py.assertEqual(u.leaves[2], py.data.chash);

        // Test Uploader.put():
        u.quick_id = 'GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN';
        u.put(py.data.leaf, py.data.chash, 2);
        py.assertEqual(
            u.request.calls,
            [
                ['open', 'PUT', url + 'GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN/2', true],
                ['setRequestHeader', 'x-dmedia-chash', py.data.chash],
                ['setRequestHeader', 'Content-Type', 'application/octet-stream'],
                ['setRequestHeader', 'Accept', 'application/json'],
                ['sendAsBinary', py.data.leaf],
            ]
        );

        // Test Uploader.post():
        u.file = DummyFile;
        u.post();
        d = {
            'quick_id': 'GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN',
            'bytes': 20202333,
        }
        py.assertEqual(
            u.request.calls,
            [
                ['open', 'POST', url, true],
                ['setRequestHeader', 'Content-Type', 'application/json'],
                ['setRequestHeader', 'Accept', 'application/json'],
                ['send', JSON.stringify(d)],
            ]
        );

        // Test Uploader.next
        var u = new Uploader('https://example.com/upload');
        u.stop = 3;
        u.i = null;
        u.leaves = [null, null, null];
        py.assertTrue(u.next());
        py.assertEqual(u.i, 0);
        py.assertTrue(u.next());
        py.assertEqual(u.i, 1);
        py.assertTrue(u.next());
        py.assertEqual(u.i, 2);
        py.assertFalse(u.next());
        py.assertEqual(u.i, 3);

        u.stop = 3;
        u.i = null;
        u.leaves = [null, 'MA3IAHUOKXR4TRG7CWAPOO7U4WCV5WJ4', null];
        py.assertTrue(u.next());
        py.assertEqual(u.i, 0);
        py.assertTrue(u.next());
        py.assertEqual(u.i, 2);
        py.assertFalse(u.next());
        py.assertEqual(u.i, 3);

        u.stop = 3;
        u.i = null;
        u.leaves = [
            'IXJTSUCYYFECGSG6JIB2R77CAJVJK4W3',
            'MA3IAHUOKXR4TRG7CWAPOO7U4WCV5WJ4',
            'FHF7KDMAGNYOVNYSYT6ZYWQLUOCTUADI',
        ];
        py.assertFalse(u.next());
        py.assertEqual(u.i, 3);

    },
}
