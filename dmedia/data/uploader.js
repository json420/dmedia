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
function hex_hmac_sha1(key, data){ return binb2hex(core_hmac_sha1(key, data));}
function b64_hmac_sha1(key, data){ return binb2b64(core_hmac_sha1(key, data));}
function str_hmac_sha1(key, data){ return binb2str(core_hmac_sha1(key, data));}

/*
 * Perform a simple self-test to see if the VM is working
 */
function sha1_vm_test()
{
  return hex_sha1("abc") == "a9993e364706816aba3e25717850c26c9cd0d89d";
}

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
 * Calculate the HMAC-SHA1 of a key and some data
 */
function core_hmac_sha1(key, data)
{
  var bkey = str2binb(key);
  if(bkey.length > 16) bkey = core_sha1(bkey, key.length * chrsz);

  var ipad = Array(16), opad = Array(16);
  for(var i = 0; i < 16; i++)
  {
    ipad[i] = bkey[i] ^ 0x36363636;
    opad[i] = bkey[i] ^ 0x5C5C5C5C;
  }

  var hash = core_sha1(ipad.concat(str2binb(data)), 512 + data.length * chrsz);
  return core_sha1(opad.concat(hash), 512 + 160);
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


// 8 MiB leaf size
leaf_size = 8 * Math.pow(2, 20);
QID_CHUNK_SIZE = Math.pow(2, 20);


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


var HashList = new Class({
    Implements: Events,

    initialize: function(file) {
        this.file = file;
        this.reader = new FileReader();
        this.reader.onload = this.on_load.bind(this);
        this.leaves = [];
        this.leaves_b32 = [];
        this.chash = null;
        this.i = 0;
        this.stop = Math.ceil(file.size / leaf_size);
    },

    run: function() {
        this.time_start = Date.now();
        this.fireEvent('progress', [0, this.file.size]);
        this.read_slice();
    },

    seconds: function() {
        return (this.time_end - this.time_start) / 1000;
    },

    read_slice: function() {
        var s = this.file.slice(this.i * leaf_size, leaf_size);
        this.reader.readAsBinaryString(s);
    },

    info: function() {
        return {
            'size': this.file.size,
            'name': this.file.name,
            'mime': this.file.type,
            'chash': this.chash,
            'leaves': this.leaves_b32,
        };
    },

    next: function() {
        this.i++;
        var completed = Math.min(this.i * leaf_size, this.file.size);
        this.fireEvent('progress', [completed, this.file.size]);
        if (this.i < this.stop) {
            this.read_slice();
            return;
        }
        this.packed_leaves = this.leaves.join('');
        var digest = str_sha1(this.packed_leaves);
        this.chash = b32encode(digest);
        this.time_end = Date.now();
        this.fireEvent('complete', this.chash);
    },

    on_load: function() {
        var digest = str_sha1(this.reader.result);
        this.leaves.push(digest);
        this.leaves_b32.push(b32encode(digest));
        this.next();
    },

});


var Uploader = new Class({
    Implements: Events,

    initialize: function(baseurl, Request) {
        if (baseurl.charAt(baseurl.length - 1) != '/') {
            this.baseurl = baseurl + '/';
        }
        else {
            this.baseurl = baseurl;
        }
        this.Request = Request || XMLHttpRequest;
        this.leaves = [];
        this.i = null;
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

    on_readystatechange: function() {

    },

    on_load: function() {
        if (this.i == null) {
            this.quick_id = b32_sha1(this.file.size.toString() + this.reader.result);
            alert(this.quick_id);
        }
        else {
            this.leaf = this.reader.result;
            this.upload_leaf(this.leaf, this.i);
        }

    },

    upload_leaf: function(data, i) {
        var chash = b32_sha1(data);
        this.leaves[i] = chash;
        this.request = new this.Request();
        var url = this.url(this.quick_id, i);
        this.request.open('PUT', url);
        this.request.setRequestHeader('x-dmedia-chash', chash);
        this.request.setRequestHeader('Content-Type', 'application/octet-stream');
        this.request.onreadystatechange = this.on_readystatechange.bind(this);
        this.request.send(data);
    },

    upload: function(file) {
        this.file = file;
        this.reader = new FileReader();
        this.reader.onload = this.on_load.bind(this);
        this.stop = Math.ceil(file.size / leaf_size);
        this.time_start = Date.now();
        var s = this.file.slice(0, QID_CHUNK_SIZE);
        this.reader.readAsBinaryString(s);
    },

    read_slice: function() {
        var s = this.file.slice(this.i * leaf_size, leaf_size);
        this.reader.readAsBinaryString(s);
    },
});


function handle(files) {
    var u = new Uploader('http://localhost:9500');
    var file = files[0];
    u.upload(file);
};
