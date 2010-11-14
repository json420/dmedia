


function on_load() {
    console.log('on_load');
    var r = new XMLHttpRequest();
    r.onreadystatechange = function () {
        console.log(r.readyState);
        console.log(r.status);
        if (r.readyState == 4) {
            console.log(JSON.parse(r.responseText));
            document.getElementById('target').textContent = r.responseText;
        }
    };
    r.open('GET', 'http://localhost:5984/dmedia');
    r.setRequestHeader('Accept', 'application/json');
    r.send();
}


window.onload = on_load;
