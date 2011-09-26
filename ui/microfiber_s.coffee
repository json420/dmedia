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
		@error = error
		console.error( classname(this) + ": " + error )

class ValueError extends Error
	constructor: (error) ->
		return super error
		
class ServerError extends Error
	constructor: (error...) ->
		super error.join(" ")

class ClientError extends Error
	constructor: (error...) ->
		super error.join(" ")
		

class HTTPConnection
	constructor: (scheme, netloc) ->
		@scheme = scheme
		@baseurl = netloc		
	
	request: (method, url, body, headers) ->	
		xhr = new XMLHttpRequest()				
		xhr.open(method, @scheme + "://" + @baseurl + url, false)
		xhr.setRequestHeader(key, value) for key, value of headers
		xhr.send(body)
		
		return xhr;
	

class CouchBase
	constructor: (url) ->
		t = urlparse(url)
		
		if t.error then return (throw new ValueError( t.error ))
		if not (t.scheme in ['http','https']) then return (throw new ValueError( 'url scheme must be http or https: ' + url ))
		
		@basepath = "/" + (if t.path.substr(-1,1) == "/" then t.path else t.path + "/")
		@basepath = @basepath.replace(/\/\/+/, "/") # replace multiple serial slashes with one
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
		

	request: (method, url, body, headers = {}) ->
		headers = extend( headers, { 'Accept' : 'application/json' } )
		
		response = @conn.request(method, url, body, headers)
		
		if response.status >= 500
			throw new ServerError(response.statusText, method, url)
		if response.status >= 400
			throw new ClientError(response.statusText, method, url)
	
		return response
		

	json: (method, obj, parts, options) ->
		url = @path(parts, options)
		body = (if obj? then dumps(obj) else null)
		headers = {'Content-type' : 'application/json'}
		
		loads(@request(method, url, body, headers).response)
		
		
	#	EXAMPLE: s.post({'_id':'foo'}, ['new_db'] )
	#				creates a new file with ID "foo" in new_db
	post: (obj, parts, options = null) ->
		@json('POST', obj, parts, options)
		
		
	#	EXAMPLE:	s.put({"_rev":"1-967a00dff5e02add41819138abb3284d", "name":"george"}, ['new_db','foo'])
	#				updates new_db/foo to have attribute name = george
	#				get the rev by using a s.get
	put: (obj, parts, options = null) ->
		@json('PUT', obj, parts, options)
		
	
	#	EXAMPLE:	s.get(['new_db','foo'])
	#				gets new_db/foo
	get: (parts, options = null) ->
		@json('GET', {}, parts, options)
		

	#	EXAMPLE: s.delete(['new_db','foo'], {'rev':"1-64f0e73673f6783391f5dceb0a41f202"})
	#				deletes new_db/foo
	#				requires a revision from using s.get
	delete: (parts, options) ->
		@json('DELETE', {}, parts, options)
		

	#	EXAMPLE: s.head(['new_db','foo'])
	#				gets the headers of that file (content-length, revision (Etag), mime for example)
	head: (parts, options) ->
		@request('HEAD', @path(parts,options), null, headers).getAllResponseHeaders()
		
		
	#	EXAMPLE: s.put_att('text/plain', 'sample text', ['new_db','foo','text'], {'rev':'2-d038fb0aac55dc19727e3125dae93787'})
	#				puts a file called "text" containing "sample text" as an attachement of new_db/foo
	#				get the revision first!
	put_att: (mime, data, parts, options) ->
		loads(@request('PUT', @path(parts, options), data, {'Content-type': mime}).response)
		
	
	#	See get, return is slightly different here - first value is mime, second is attachement contents
	#
	get_att: (parts, options) ->
		response = @request('GET', @path(parts,options))
		return [response.getResponseHeader('Content-type'), response.response]
		
		

class Server extends CouchBase
	database: (name, ensure = false) ->
		return new Database(@url + name, ensure)
		
		
class Database extends CouchBase
	constructor: (url, ensure = false) ->
		super url
		if ensure is true
			return @ensure()
		
		
	ensure: ->
		try
			@request('PUT', @path([]))
		catch e
			console.log(e)
			if ! e.error.match("Precondition Failed")
				throw e
			else
				return true;
		
		return true
		
	
	# requires a callback!
	# probably doesn't work
	save: (doc) ->
		response = @post(doc)

		doc['_id'] = response['id']
		doc['_rev'] = response['rev']

		return response
		
	
	bulksave: (docs) ->
		rows = @post({'docs': docs, 'all_or_nothing': true}, '_bulk_docs')	
		[ doc['_id'] = rows[i]['id'], doc['_rev'] = rows[i]['rev'] ] for i, doc in docs
	
		return rows

		
		
this.CouchBase = CouchBase
this.Server = Server
this.Database = Database
this.HTTPConnection = HTTPConnection

#s = new Server('localhost:5984')
#s.get( (() -> (return)), ['_users'] )
