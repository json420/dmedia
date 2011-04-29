# copies all non-existant keys from b to a.
# a = { name: 'bob', age: 42 }
# b = { name: 'george', color: 'blue' }
# extend(a,b) -> { name: 'bob', age: 42, color: 'blue' }
this.extend = extend = (a, b) -> (a[key] = value for key, value of b when not a[key]?); return a

# gets the classname of an object.
this.classname = classname = (c) ->
	if c? and c.constructor? and c.constructor.toString?
		arr = c.constructor.toString().match(/function\s*(\w+)/);
		if arr? and arr.length >= 2 then return arr[1]
	return "undefined"


this.dumps = dumps = (a) -> JSON.stringify(a)
this.loads = loads = (a) -> JSON.parse(a) #! may requires changes to work cross-browser

# copied pretty much verbatim from the jQuery library, so it's knonw to work
this.query = query = (o) ->
	s = []
	add = (key, value) -> (s.push( encodeURIComponent(key) + "=" + encodeURIComponent(if typeof(value) == "function" then value() else value) ))
	buildParams = (prefix, obj) =>
		if obj.push? and obj.length then buildParams(prefix + "[" + (if typeof(value) == "object" or value.length? then key else "") + "]", value) for key, value of obj # non-empty arrays
		else if obj? and typeof(obj) == "object" # objects of some sort
			if obj.push? or obj == {} then add(prefix, "") # empty objects/arrays
			else buildParams( [prefix, "[", key, "]"].join(""), value) for key, value of obj # serialize objects/items
		else add(prefix, obj) # numbers, nulls, etc
	if o.push? then add(key, value) for key, value of o
	else buildParams(key, value) for key, value of o
	return s.join("&").replace( "%20", "+" )


# THIS ONLY WORKS WITH ABSOLUTE URLS
# /somepath WILL NOT WORK
this.urlparse = urlparse = (url) ->
	# this is a really shitty regex... feel free to change it
	# BUT, it works, at least
	regex = /(([a-z]+):\/\/)?([^\/:]+(:([0-9]+))?)(\/)?([^?#]+)?((\?)?([^#]+))?#?(.*)/i
	bits = url.match(regex)
		
	if not bits or bits[3].match(/([^a-z0-9-:]+)/)
		return { error: 'malformed url - ' + url }
	
	t = {}
	t.scheme = (if bits[2] then bits[2] else 'http')
	t.netloc = bits[3]
	t.port   = (if bits[5] then bits[5] else 80)
	t.path   = (if bits[7] then bits[7] else '')
	t.params = (if bits[10] then bits[10] else '')
	t.query  = (if bits[11] then bits[11] else '')
	return t
	

class Error
	constructor: (error) ->
		console.log classname(this), error
		return null

class ValueError extends Error
	constructor: (error) ->
		return super error
		
class ServerError extends Error
	constructor: (error) ->
		super error

class ClientError extends Error
	constructor: (error) ->
		super error
		
class PreconditionFailedError
	constructor: (error) ->
		super error


class HTTPConnection
	constructor: (scheme, netloc) ->
		@scheme = scheme
		@baseurl = netloc		
	
	request: (callback, method, url, body, headers) ->	
		xhr = new XMLHttpRequest()
		xhr.onreadystatechange = () ->
			if xhr.readyState == 4
				callback(xhr)
				
		xhr.open(method, @scheme + "://" + @baseurl + url, true)
		xhr.setRequestHeader(key, value) for key, value of headers
		xhr.send(body)
	

class CouchBase
	constructor: (url) ->
		t = urlparse(url)
		
		if t.error then return new ValueError( t.error )
		if not (t.scheme in ['http','https']) then return new ValueError( 'url scheme must be http or https: ' + url )
		
		@basepath = (if t.path.substr(-1,1) == "/" then t.path else t.path + "/")
		@url = [t.scheme, '://', t.netloc, @basepath].join('')			
		@conn = new HTTPConnection(t.scheme, t.netloc)
			
			
	toString: ->
		return classname(this) + "(" + @url + ")"
		
		
	path: (parts, options = {}) ->
		# if parts is not an array, make options it
		if not parts.push?
			extend(parts,options) #! may fail, test
			parts = []
			
		url = @basepath
		options = query(options) #!
		
		if parts.push then url += parts.join("/")
		if options then url += "?" + options
			
		return url
		
	# differs from Py due to asynchronicity - requires a callback as first argument
	# callback can be an object like { success: function, error: function }, or just a success function
	request: (callback, method, url, body, headers = {}) ->
		headers = extend( headers, { 'Accept' : 'application/json' } )
		
		cb = callback
		eb = null
		
		if callback.success?
			cb = callback.success
			
		if callback.error?
			eb = callback.error
		
			
		ic = (response) ->
			if response.status >= 500
				error = new ServerError(response, method, url)
			else if response.status >= 400
				error = new ClientError(response, method, url)
			if error?
				if eb? then eb(error)
			else
				# only call callback if no error
				cb(response)
		
		@conn.request(ic, method, url, body, headers)
	
	
	# also differs from Py version, requires callback as first argument
	# callback can be an object like { success: function, error: function }, or just a success function	
	json: (callback, method, obj, parts, options) ->
		url = @path(parts, options)
		body = (if obj? then dumps(obj) else null)
		headers = {'Content-type' : 'application/json'}
		
		@request(callback, method, url, body, headers)
		
	
	# interim, shortens code
	# NOT IN PY
	read_json: (callback, method, obj, parts, options) ->
		if callback.success?
			cb = callback.success #! might fail, test heavily
		else
			cb = callback
			callback = {}
		
		callback.success = (response) => cb(loads(response.response))
		@json(callback, method, obj, parts, options)
		
	
	# differs from Py, requires callback
	# callback can be an object like { success: function, error: function }, or just a success function
	#
	#	EXAMPLE: s.post( cb, {'_id':'foo'}, ['new_db'] )
	#				creates a new file with ID "foo" in new_db
	post: (callback, obj, parts, options = null) ->
		@read_json(callback, 'POST', obj, parts, options)
		
		
	# differs from Py, requires callback
	# callback can be an object like { success: function, error: function }, or just a success function
	#
	#	EXAMPLE:	s.put(cb, {"_rev":"1-967a00dff5e02add41819138abb3284d", "name":"george"}, ['new_db','foo'])
	#				updates new_db/foo to have attribute name = george
	#				get the rev by using a s.get
	put: (callback, obj, parts, options = null) ->
		@read_json(callback, 'PUT', obj, parts, options)
		
	
	# differs from Py, requires callback
	# callback can be an object like { success: function, error: function }, or just a success function
	#
	#	EXAMPLE:	s.get(cb, ['new_db','foo'])
	#				gets new_db/foo
	get: (callback, parts, options = null) ->
		@read_json(callback, 'GET', {}, parts, options)
		

	# differs from Py, requires callback
	# callback can be an object like { success: function, error: function }, or just a success function
	#
	#	EXAMPLE: s.delete(cb, ['new_db','foo'], {'rev':"1-64f0e73673f6783391f5dceb0a41f202"})
	#				deletes new_db/foo
	#				requires a revision from using s.get
	delete: (callback, parts, options) ->
		@read_json(callback, 'DELETE', {}, parts, options)
		

	# differs from Py, requires callback
	# callback can be an object like { success: function, error: function }, or just a success function
	#
	#	EXAMPLE: s.head(cb, ['new_db','foo'])
	#				gets the headers of that file (content-length, revision (Etag), mime for example)
	head: (callback, parts, options) ->
		if callback.success?
			cb = callback.success # might fail, test!
		else
			cb = callback
			callback = {}
		
		callback.success = (response) => cb(response.getAllResponseHeaders())
		@json(callback, 'HEAD', {}, parts, options)
		
	
	# requires callback
	#
	#	EXAMPLE: s.put_att(cb, 'text/plain', 'sample text', ['new_db','foo','text'], {'rev':'2-d038fb0aac55dc19727e3125dae93787'})
	#				puts a file called "text" containing "sample text" as an attachement of new_db/foo
	#				get the revision first!
	put_att: (callback, mime, data, parts, options) ->
		url = @path(parts, options)
		headers = {'Content-type': mime}

		if callback.success?
			cb = callback.success #! might fail, test heavily
		else
			cb = callback
			callback = {}
		
		callback.success = (response) => cb(loads(response.response))		
		
		@request(callback, 'PUT', url, data, headers)
	
	
	# requires callback
	#
	#	See get, callback is slightly different here - first argument is mime, second if attachement contents
	#
	get_att: (callback, parts, options) ->
		url = @path(parts, options)

		if callback.success?
			cb = callback.success #! might fail, test heavily
		else
			cb = callback
			callback = {}
		
		callback.success = (response) => cb(response.getResponseHeader('Content-Type'), response.response)		
		
		@request(callback, 'GET', url)
		
		

class Server extends CouchBase
	database: (name, ensure = false) ->
		return new Database(@url + name, ensure)
		
		
class Database extends CouchBase
	constructor: (url, ensure = false) ->
		super url
		if ensure is true
			@ensure()
		
		
	ensure: ->
		callback = {success: (() -> return), error: (() -> new PreconditionFailedError())}
		@put(callback, {}, [])
		
	
	# requires a callback!
	# probably doesn't work
	save: (callback, doc) ->
		cb = callback #! might overwrite, test!
		cb.success = (request) =>
			doc.update( {'_id': request.id, '_rev': request.rev } ) #! uses arrays in Py, test
			if callback.success? then callback.success(request) else callback(request)
			
		@put(callback, doc)
		
	
	bulksave: (callback, docs) ->
		cb = callback #! might overwrite, test!
		cb.success = (request) =>
			doc.update({'_id': request[i].id, '_rev': request[i].rev}) for i, doc of docs #! unsure about zip method, best guess possible...
			if callback.success? then callback.success(doccs) else callback(docs)
			
		@put(callback, {'docs': docs, 'all_or_nothing': true}, '_bulk_docs')	
		
		
this.CouchBase = CouchBase
this.Server = Server
this.Database = Database
this.HTTPConnection = HTTPConnection

#s = new Server('localhost:5984')
#s.get( (() -> (return)), ['_users'] )
