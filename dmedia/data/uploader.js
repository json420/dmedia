// 8 MiB leaf size
leaf_size = 8 * Math.pow(2, 20);

function handle(files) {
    var display = document.getElementById('display');
    var file = files[0];
    var d = {
        'name': file.name,
        'size': file.size,
        'mime': file.type
    };
    display.textContent = JSON.stringify(d);
};
