"use strict";

function $(id) {
    /*
    Return the element with id="id".

    If id is an Element, it is returned unchanged.

    Examples:

    >>> $('browser');
    <div id="browser" class="box">
    >>> var el = $('browser');
    undefined
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
