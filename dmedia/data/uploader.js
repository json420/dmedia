LEAF_SIZE = 8 * Math.pow(2, 20);  // 8 MiB leaf size
QID_CHUNK_SIZE = Math.pow(2, 20);  // quick_id() uses first MiB of file

/*
Base32-encoder compliments of:
    http://forthescience.org/blog/2010/11/30/base32-encoding-in-javascript/
*/
var b32encode = function(s) {
    /* encodes a string s to base32 and returns the encoded string */
    var alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

    var parts = [];
    var quanta= Math.floor((s.length / 5));
    var leftover = s.length % 5;

    if (leftover != 0) {
        for (var i = 0; i < (5-leftover); i++) {
            s += '\x00';
        }
        quanta += 1;
    }

    for (i = 0; i < quanta; i++) {
        parts.push(alphabet.charAt(s.charCodeAt(i*5) >> 3));
        parts.push(alphabet.charAt( ((s.charCodeAt(i*5) & 0x07) << 2) | (s.charCodeAt(i*5+1) >> 6)));
        parts.push(alphabet.charAt( ((s.charCodeAt(i*5+1) & 0x3F) >> 1) ));
        parts.push(alphabet.charAt( ((s.charCodeAt(i*5+1) & 0x01) << 4) | (s.charCodeAt(i*5+2) >> 4)));
        parts.push(alphabet.charAt( ((s.charCodeAt(i*5+2) & 0x0F) << 1) | (s.charCodeAt(i*5+3) >> 7)));
        parts.push(alphabet.charAt( ((s.charCodeAt(i*5+3) & 0x7F) >> 2)));
        parts.push(alphabet.charAt( ((s.charCodeAt(i*5+3) & 0x03) << 3) | (s.charCodeAt(i*5+4) >> 5)));
        parts.push(alphabet.charAt( ((s.charCodeAt(i*5+4) & 0x1F) )));
    }

    var replace = 0;
    if (leftover == 1) replace = 6;
    else if (leftover == 2) replace = 4;
    else if (leftover == 3) replace = 3;
    else if (leftover == 4) replace = 1;

    for (i = 0; i < replace; i++) parts.pop();
    for (i = 0; i < replace; i++) parts.push("=");

    return parts.join("");
};


function b32_sha1(s) {
    // Return base32-encoded sha1 hash of *s*.
    return b32encode(rstr_sha1(s));
}


function quick_id(size, chunk) {
    return b32_sha1(size.toString() + chunk);
}


// FIXME: remove, for testing only
function log() {
    var parent = document.getElementById('log');
    if (! parent) {
        return;
    }
    var args = Array.prototype.slice.call(arguments);
    var msg = args.join(' ');
    var pre = document.createElement('pre');
    pre.textContent = msg;
    parent.appendChild(pre);
}


function Uploader(baseurl, Request) {
        var baseurl = baseurl || 'upload/';
        if (baseurl.charAt(baseurl.length - 1) != '/') {
            var baseurl = baseurl + '/';
        }
        this.baseurl = baseurl;
        this.Request = Request || XMLHttpRequest;
        this.leaves = [];
        this.i = null;
        this.retries = 0;
}

Uploader.prototype = {

    new_request: function() {
        this.request = new this.Request();
        if (this.on_readystatechange.bind) {
            this.request.onreadystatechange = this.on_readystatechange.bind(this);
        }
        else {
            this.request.onreadystatechange = this.on_readystatechange;
        }
    },

    post: function(obj, quick_id) {
        // Start, resume, or finish a multipart upload
        var obj = obj || {};
        obj['quick_id'] = this.quick_id;
        obj['bytes'] = this.file.size;
        this.new_request();
        this.request.open('POST', this.url(quick_id), true);
        this.request.setRequestHeader(
            'Content-Type',
            'application/json; charset=UTF-8'
        );
        this.request.setRequestHeader('Accept', 'application/json');
        this.request.send(JSON.stringify(obj));
    },

    put: function(data, chash, i) {
        // Upload a leaf
        this.new_request();
        var url = this.url(this.quick_id, i);
        this.request.open('PUT', url, true);
        this.request.setRequestHeader('x-dmedia-chash', chash);
        this.request.setRequestHeader('Content-Type', 'application/octet-stream');
        this.request.setRequestHeader('Accept', 'application/json');
        if (this.request.sendAsBinary) {
            this.request.sendAsBinary(data);
        }
        else {
            this.request.send(this.slice);
        }
    },

    url: function(quick_id, leaf) {
        /*
        Construct URL relative to baseurl.

        Examples:
            u.url();
            u.url('GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN');
            u.url('GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN', 17);
        */
        if (typeof quick_id != 'string' || quick_id.length < 1) {
            return this.baseurl;
        }
        if (typeof leaf == 'number') {
            return this.baseurl + quick_id + '/' + leaf;
        }
        return this.baseurl + quick_id;
    },

    on_load: function() {
        // Handle FileReader.onload
        log('on_load');
        if (this.i == null) {
            this.quick_id  = quick_id(this.file.size, this.reader.result);
            log('quick_id', this.quick_id);
            this.send();
        }
        else {
            var chash = this.hash_leaf(this.reader.result, this.i);
            log('leaf', this.i, chash);
            this.send();
        }
    },

    on_readystatechange: function(event) {
        // Handle XMLHttpRequest.onreadystatechange
        if (this.request.readyState != 4) {
            // We only care about completed requests
            return;
        }
        log('readystatechange', this.request.status, this.request.statusText);
        log(this.request.responseText);
        if (this.request.status == 409) {
            // Server lost track of upload, we need to start over:
            log('Session lost, restarting...');
            this.i = null;
            this.retry();
            return;
        }
        if (this.request.status == 412) {
            // Leaf was corrupted in transit, try uploading again:
            log('CORRUPTED - retrying leaf upload');
            this.send();
            return;
        }
        if (this.request.status >= 400) {
            // Other unknown error, retry the last request, whatever it was:
            log('ERROR - retrying request');
            this.retry();  // retry the request
            return;
        }
        if (this.i >= this.stop) {
            log('upload complete', this.elapsed());
            return;
        }
        if (this.i == null) {
            try {
                var obj = JSON.parse(this.request.responseText);
                this.leaves = obj['leaves'];
            }
            catch (e) {
                log(e);
                this.retry();
                return
            }
        }
        if (this.next()) {
            this.read_slice();
        }
        else {
            this.send();
        }
    },

    hash_leaf: function(data, i) {
        var chash = b32_sha1(data);
        this.leaves[i] = chash;
        return chash;
    },

    upload: function(file) {
        this.file = file;
        this.reader = new FileReader();
        this.reader.onload = this.on_load.bind(this);
        this.stop = Math.ceil(file.size / LEAF_SIZE);
        this.time_start = Date.now();
        var s = this.file.slice(0, QID_CHUNK_SIZE);
        this.reader.readAsBinaryString(s);
    },

    elapsed: function() {
        return (Date.now() - this.time_start) / 1000;
    },

    read_slice: function() {
        this.reader = new FileReader();
        this.reader.onload = this.on_load.bind(this);
        this.slice = this.file.slice(this.i * LEAF_SIZE, LEAF_SIZE);
        this.reader.readAsBinaryString(this.slice);
    },

    retry: function() {
        if (this.retries < 5) {
            this.retries++;
            this.send();
        }
    },

    send: function() {
        // Send (or re-send) a request
        if (this.i == null) {
            this.post();
            return;
        }
        if (this.i < this.stop) {
            this.put(this.reader.result, this.leaves[this.i], this.i);
            return;
        }
        obj = {
            'leaves': this.leaves,
            'name': this.file.name,
            'mime': this.file.type,
        }
        this.post(obj, this.quick_id);
    },

    next: function() {
        /*
        Move this.i to next leaf that needs uploading.

        Returns true if a leaf needs to be uploaded (in which case this.i will
        be set at the index of the leaf to upload).  Returns false if all leaves
        have been uploaded.
        */
        while (true) {
            if (this.i == null) {
                this.i = 0;
            }
            else {
                this.i++;
            }
            if (this.i >= this.stop) {
                return false;
            }
            if (this.leaves[this.i] == null) {
                return true;
            }
        }
    },

        /*
        var completed = Math.min(this.i * LEAF_SIZE, this.file.size);
        this.fireEvent('progress', [completed, this.file.size]);
        */
}
