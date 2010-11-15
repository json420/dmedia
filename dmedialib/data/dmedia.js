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
        var thm = row.doc.thumbnail;
        var data = 'data:' + thm.content_type + ';base64,' + thm.data;
        var img = new Element('img', {src: data, width: '192', height: '108'});
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
        {reduce: false, include_docs: true, limit: 50},
        {keys: ['mov']}
    );
}
