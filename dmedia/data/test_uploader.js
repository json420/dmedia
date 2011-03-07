py.test_b32encode = function() {
    py.data.pairs.forEach(function(d) {
        py.assertEqual(b32encode(d.src), d.dst);
    });
};
