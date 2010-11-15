var CouchRequest = new Class({
    initialize: function(callback) {
        console.assert(typeof(callback) == 'function');
        this.callback = callback;
        this.request =  new XMLHttpRequest();
        this.request.onreadystatechange = this._on_readystatechange.bind(this);
    },

    _on_readystatechange: function() {
        if (this.request.readyState != 4) {
            return;
        }
        if (this.request.status == 200) {
            console.time('JSON.parse');
            var object = JSON.parse(this.request.responseText);
            console.timeEnd('JSON.parse');
            var response = {
                success: true,
                request: this.request,
                string: this.request.responseText,
                object: object,
            };
            this.callback(response);
        }
        else {
            this.callback({
                success: false,
                request: this.request,
            });
        }
    },

    _open: function(method, url, options) {
        this.request.open(method, url + '?' + Object.toQueryString(options));
        this.request.setRequestHeader('Accept', 'application/json');
    },

    _send_json: function(body) {
        this.request.setRequestHeader('Content-Type', 'application/json; charset=utf-8');
        this.request.send(JSON.stringify(body));
    },

    get: function(url, options) {
        this._open('GET', url, options);
        this.request.send();
    },

    post: function(url, options, body) {
        this._open('POST', url, options);
        this._send_json(body);
    },
});


function minsec(seconds) {
    if (typeof(seconds) != 'number') {
        return null;
    }
    var m = (seconds / 60).toFixed().toString();
    var s = (seconds % 60).toString();
    if (s.length == 1) {
        s = '0' + s;
    }
    return m + ':' + s;
}
console.assert(minsec('hello') === null);
console.assert(minsec(0) == '0:00');
console.assert(minsec(3) == '0:03');
console.assert(minsec(17) == '0:17');
console.assert(minsec(69) == '1:09');


function todata(obj) {
    if (typeof(obj) != 'object') {
        return null;
    }
    if (typeof(obj.content_type) != 'string') {
        return null;
    }
    if (typeof(obj.data) != 'string') {
        return null;
    }
    return 'data:' + obj.content_type + ';base64,' + obj.data;
}
console.assert(todata(17) === null);
console.assert(todata({}) === null);
console.assert(todata({content_type: true, data: 'foo'}) === null);
console.assert(todata({content_type: 'image/jpeg', data: 17}) === null);
console.assert(todata({content_type: 'image/jpeg', data: 'foo'}) == 'data:image/jpeg;base64,foo');


/*
function cb(ret) {
    console.time('Redraw');
    var table = $('target');
    while (table.rows.length) {
        table.deleteRow(-1);
    }
    ret.object.rows.forEach(function(row) {
        var thm = row.doc.thumbnail;
        var data = 'data:' + thm.content_type + ';base64,' + thm.data;
        var tr = new Element('tr');
        var td = new Element('td');
        var img = new Element('img', {src: data, width: '192', height: '108'});
        td.appendChild(img);
        tr.appendChild(td);
        table.appendChild(tr);
    });
    console.timeEnd('Redraw');
}
*/

function cb(ret) {
    console.time('Redraw');
    var replacement = new Element('div', {id: 'target'});
    ret.object.rows.forEach(function(row) {
        var doc = row.doc;
        var img = new Element('img', {
            id: doc._id,
            src: todata(doc.thumbnail),
            width: '192',
            height: '108',
            title: minsec(doc.duration),
        });
        replacement.appendChild(img);
    });
    $('stage').replaceChild(replacement, $('target'));
    console.timeEnd('Redraw');
}

function testClick() {
    var r = new CouchRequest(cb);
    //r.post('/dmedia/_design/ext/_view/ext?reduce=false',);
    r.post(
        '/dmedia/_design/ext/_view/ext',
        {reduce: false, include_docs: true},
        {keys: ['mov']}
    );
}
