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
