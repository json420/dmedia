LEAF_SIZE = 8 * Math.pow(2, 20);  // 8 MiB leaf size
QID_CHUNK_SIZE = Math.pow(2, 20);  // quick_id() uses first MiB of file

/*
 * A JavaScript implementation of the Secure Hash Algorithm, SHA-1, as defined
 * in FIPS PUB 180-1
 * Version 2.1a Copyright Paul Johnston 2000 - 2002.
 * Other contributors: Greg Holt, Andrew Kepert, Ydnar, Lostinet
 * Distributed under the BSD License
 * See http://pajhome.org.uk/crypt/md5 for details.
 */

/*
 * Configurable variables. You may need to tweak these to be compatible with
 * the server-side, but the defaults work in most cases.
 */
var hexcase = 0;  /* hex output format. 0 - lowercase; 1 - uppercase        */
var b64pad  = "="; /* base-64 pad character. "=" for strict RFC compliance   */
var chrsz   = 8;  /* bits per input character. 8 - ASCII; 16 - Unicode      */

/*
 * These are the functions you'll usually want to call
 * They take string arguments and return either hex or base-64 encoded strings
 */
function hex_sha1(s){return binb2hex(core_sha1(str2binb(s),s.length * chrsz));}
function b64_sha1(s){return binb2b64(core_sha1(str2binb(s),s.length * chrsz));}
function str_sha1(s){return binb2str(core_sha1(str2binb(s),s.length * chrsz));}

/*
 * Calculate the SHA-1 of an array of big-endian words, and a bit length
 */
function core_sha1(x, len)
{
  /* append padding */
  x[len >> 5] |= 0x80 << (24 - len % 32);
  x[((len + 64 >> 9) << 4) + 15] = len;

  var w = Array(80);
  var a =  1732584193;
  var b = -271733879;
  var c = -1732584194;
  var d =  271733878;
  var e = -1009589776;

  for(var i = 0; i < x.length; i += 16)
  {
    var olda = a;
    var oldb = b;
    var oldc = c;
    var oldd = d;
    var olde = e;

    for(var j = 0; j < 80; j++)
    {
      if(j < 16) w[j] = x[i + j];
      else w[j] = rol(w[j-3] ^ w[j-8] ^ w[j-14] ^ w[j-16], 1);
      var t = safe_add(safe_add(rol(a, 5), sha1_ft(j, b, c, d)),
                       safe_add(safe_add(e, w[j]), sha1_kt(j)));
      e = d;
      d = c;
      c = rol(b, 30);
      b = a;
      a = t;
    }

    a = safe_add(a, olda);
    b = safe_add(b, oldb);
    c = safe_add(c, oldc);
    d = safe_add(d, oldd);
    e = safe_add(e, olde);
  }
  return Array(a, b, c, d, e);

}

/*
 * Perform the appropriate triplet combination function for the current
 * iteration
 */
function sha1_ft(t, b, c, d)
{
  if(t < 20) return (b & c) | ((~b) & d);
  if(t < 40) return b ^ c ^ d;
  if(t < 60) return (b & c) | (b & d) | (c & d);
  return b ^ c ^ d;
}

/*
 * Determine the appropriate additive constant for the current iteration
 */
function sha1_kt(t)
{
  return (t < 20) ?  1518500249 : (t < 40) ?  1859775393 :
         (t < 60) ? -1894007588 : -899497514;
}

/*
 * Add integers, wrapping at 2^32. This uses 16-bit operations internally
 * to work around bugs in some JS interpreters.
 */
function safe_add(x, y)
{
  var lsw = (x & 0xFFFF) + (y & 0xFFFF);
  var msw = (x >> 16) + (y >> 16) + (lsw >> 16);
  return (msw << 16) | (lsw & 0xFFFF);
}

/*
 * Bitwise rotate a 32-bit number to the left.
 */
function rol(num, cnt)
{
  return (num << cnt) | (num >>> (32 - cnt));
}

/*
 * Convert an 8-bit or 16-bit string to an array of big-endian words
 * In 8-bit function, characters >255 have their hi-byte silently ignored.
 */
function str2binb(str)
{
  var bin = Array();
  var mask = (1 << chrsz) - 1;
  for(var i = 0; i < str.length * chrsz; i += chrsz)
    bin[i>>5] |= (str.charCodeAt(i / chrsz) & mask) << (32 - chrsz - i%32);
  return bin;
}

/*
 * Convert an array of big-endian words to a string
 */
function binb2str(bin)
{
  var str = "";
  var mask = (1 << chrsz) - 1;
  for(var i = 0; i < bin.length * 32; i += chrsz)
    str += String.fromCharCode((bin[i>>5] >>> (32 - chrsz - i%32)) & mask);
  return str;
}

/*
 * Convert an array of big-endian words to a hex string.
 */
function binb2hex(binarray)
{
  var hex_tab = hexcase ? "0123456789ABCDEF" : "0123456789abcdef";
  var str = "";
  for(var i = 0; i < binarray.length * 4; i++)
  {
    str += hex_tab.charAt((binarray[i>>2] >> ((3 - i%4)*8+4)) & 0xF) +
           hex_tab.charAt((binarray[i>>2] >> ((3 - i%4)*8  )) & 0xF);
  }
  return str;
}

/*
 * Convert an array of big-endian words to a base-64 string
 */
function binb2b64(binarray)
{
  var tab = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  var str = "";
  for(var i = 0; i < binarray.length * 4; i += 3)
  {
    var triplet = (((binarray[i   >> 2] >> 8 * (3 -  i   %4)) & 0xFF) << 16)
                | (((binarray[i+1 >> 2] >> 8 * (3 - (i+1)%4)) & 0xFF) << 8 )
                |  ((binarray[i+2 >> 2] >> 8 * (3 - (i+2)%4)) & 0xFF);
    for(var j = 0; j < 4; j++)
    {
      if(i * 8 + j * 6 > binarray.length * 32) str += b64pad;
      else str += tab.charAt((triplet >> 6*(3-j)) & 0x3F);
    }
  }
  return str;
}


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
    return b32encode(str_sha1(s));
}

function on_progress(completed, total) {
    var p = parseInt(completed / total * 100);
    document.getElementById('progress').textContent = 'Hashing: ' + p + '% ' + completed + ' of ' + total + ' bytes';
}

function handle(files) {
    var display = document.getElementById('display');
    display.innerHTML = '';

    function addpre(text) {
        var pre = document.createElement('pre');
        pre.textContent = text;
        display.appendChild(pre);
    };

    var file = files[0];
    addpre('name = ' + file.name);
    addpre('size = ' + file.size);
    addpre('mime = ' + file.type);

    var h = new HashList(file);
    h.addEvent('progress', on_progress);
    h.addEvent('complete', function(chash) {
        addpre(h.seconds() + ' seconds');
        addpre('chash = ' + chash);
        addpre(JSON.stringify(h.info()));
    });
    h.run();
};


function quick_id(size, chunk) {
    return b32_sha1(size.toString() + chunk);
}


var Uploader = new Class({
    Implements: Events,

    initialize: function(baseurl, Request) {
        var baseurl = baseurl || 'upload/';
        if (baseurl.charAt(baseurl.length - 1) != '/') {
            var baseurl = baseurl + '/';
        }
        this.baseurl = baseurl;
        this.Request = Request || XMLHttpRequest;
        this.leaves = [];
        this.i = null;
    },

    log: function() {
        var args = Array.prototype.slice.call(arguments);
        var msg = args.join(' ');
        var parent = document.getElementById('log');
        var pre = document.createElement('pre');
        pre.textContent = msg;
        parent.appendChild(pre);
    },

    new_request: function() {
        this.request = new this.Request();
        this.request.onreadystatechange = this.on_readystatechange.bind(this);
        return this.request;
    },

    post: function(obj, quick_id) {
        var obj = obj || {};
        obj['quick_id'] = this.quick_id;
        obj['bytes'] = this.file.size;
        var request = this.new_request();
        request.open('POST', this.url(quick_id), true);
        request.setRequestHeader('Content-Type', 'application/json');
        //request.setRequestHeader('Accept', 'application/json');
        request.send(JSON.stringify(obj));
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

    retry: function() {

    },

    on_readystatechange: function(state) {
        if (this.request.readyState != 4) {
            return;
        }
        this.log('readystatechange', this.request.status, this.request.statusText);
        this.log(this.request.responseText);
        try {
            var obj = JSON.parse(this.request.responseText);
        }
        catch (e) {
            this.log(e);
        }
        if (this.i == null) {
            this.leaves = obj['leaves'];
            this.i = 0;
            this.read_slice();
        }
    },

    on_load: function() {
        if (this.i == null) {
            this.preable = this.reader.result;
            this.quick_id  = quick_id(this.file.size, this.preamble);
            this.log('quick_id', this.quick_id);
            this.post();
        }
        else {
            this.leaf = this.reader.result;
            var chash = this.hash_leaf(this.leaf, this.i);
            this.log('leaf', this.i, chash);
            this.upload_leaf(this.leaf, chash, this.i);
        }
    },

    hash_leaf: function(data, i) {
        var chash = b32_sha1(data);
        this.leaves[i] = chash;
        return chash;
    },

    upload_leaf: function(data, chash, i) {
        this.request = this.new_request();
        var url = this.url(this.quick_id, i);
        this.request.open('PUT', url, true);
        this.request.setRequestHeader('x-dmedia-chash', chash);
        this.request.setRequestHeader('Content-Type', 'application/octet-stream');
        this.request.sendAsBinary(data);
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
        var s = this.file.slice(this.i * LEAF_SIZE, LEAF_SIZE);
        this.reader.readAsBinaryString(s);
    },

    next: function() {
        this.i++;
        var completed = Math.min(this.i * LEAF_SIZE, this.file.size);
        this.fireEvent('progress', [completed, this.file.size]);
        if (this.i < this.stop) {
            this.read_slice();
            return;
        }
    },
});


function handle(files) {
    var u = new Uploader('/');
    var file = files[0];
    u.upload(file);
};
