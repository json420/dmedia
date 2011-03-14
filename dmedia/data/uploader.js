LEAF_SIZE = 8 * Math.pow(2, 20);  // 8 MiB leaf size
QID_CHUNK_SIZE = Math.pow(2, 20);  // quick_id() uses first MiB of file


function b32_sha1(s) {
    // Return base32-encoded sha1 hash of *s*.
    return b32encode(rstr_sha1(s));
}


function quick_id(size, chunk) {
    return b32_sha1(size.toString() + chunk);
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
    onrequest: null,
    onprogress: null,

    upload: function(file) {
        // Upload a file
        this.file = file;
        this.reader = new FileReader();
        this.reader.onload = this.on_load.bind(this);
        this.stop = Math.ceil(file.size / LEAF_SIZE);
        this.time_start = Date.now();
        var s = this.file.slice(0, QID_CHUNK_SIZE);
        this.reader.readAsBinaryString(s);
    },

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
        if (this.i == null) {
            this.quick_id  = quick_id(this.file.size, this.reader.result);
            this.send();
        }
        else {
            var chash = this.hash_leaf(this.reader.result, this.i);
            this.send();
        }
    },

    on_readystatechange: function(event) {
        // Handle XMLHttpRequest.onreadystatechange
        if (this.request.readyState != 4) {
            // We only care about completed requests
            return;
        }
        if (this.onrequest) {
            this.onrequest(this.request);
        }
        if (this.request.status == 409) {
            // Server lost track of upload, we need to start over:
            this.i = null;
            this.retry();
            return;
        }
        if (this.request.status == 412) {
            // Leaf was corrupted in transit, try uploading again:
            this.send();
            return;
        }
        if (this.request.status >= 400) {
            // Other unknown error, retry the last request, whatever it was:
            this.retry();  // retry the request
            return;
        }
        if (this.i >= this.stop) {
            return;
        }
        if (this.i == null) {
            try {
                var obj = JSON.parse(this.request.responseText);
                this.leaves = obj['leaves'];
            }
            catch (e) {
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
            if (this.onprogress) {
                var completed = Math.min(this.i * LEAF_SIZE, this.file.size);
                this.onprogress(completed, this.file.size);
            }
            if (this.i >= this.stop) {
                return false;
            }
            if (this.leaves[this.i] == null) {
                return true;
            }
        }
    },
}
