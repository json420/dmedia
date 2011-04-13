"use strict";

/*
A very minimal "unframework" to capture just a few important patterns, make a
few things nicer, less verbose.  But this is *not* a band aid for browser
compatibility, nor for the JavaScript language.

Note that couch.js should never depend on anything in here.
*/

function $(id) {
    /*
    Return the element with id="id".

    If `id` is an Element, it is returned unchanged.

    Examples:

    >>> $('browser');
    <div id="browser" class="box">
    >>> var el = $('browser');
    >>> $(el);
    <div id="browser" class="box">

    */
    if (id instanceof Element) {
        return id;
    }
    return document.getElementById(id);
}

function $el(tag, attributes) {
    /*
    Convenience function to create a new DOM element and set its attributes.

    Examples:

    >>> $el('img');
    <img>
    >>> $el('img', {'class': 'thumbnail', 'src': 'foo.png'});
    <img class="thumbnail" src="foo.png">

    */
    var el = document.createElement(tag);
    if (attributes) {
        var key;
        for (key in attributes) {
            el.setAttribute(key, attributes[key]);
        }
    }
    return el;
}

function $replace(incumbent, replacement) {
    /*
    Replace `incumbent` with `replacement`.

    `incumbent` can be an element or id, `replacement` must be an element.

    Returns the element `incumbent`.
    */
    var incumbent = $(incumbent);
    return incumbent.parentNode.replaceChild(replacement, incumbent);
}

function $hide(el) {
    $(el).classList.add('hide');
}

function $show(el) {
    $(el).classList.remove('hide');
}

function $appendEach(parent, viewresult, func) {
    viewresult.rows.forEach(function(row) {
        parent.appendChild(func(row.doc));
    });
}

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
