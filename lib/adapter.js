/**
 * Module Dependencies
 */

var _                 = require('lodash'),
    async             = require('async'),
    rest              = require('restler'),
    waterlineCriteria = require('waterline-criteria'),
    Aggregate         = require('./aggregates');

/**
 * waterline-sails-rest
 *
 * Most of the methods below are optional.
 *
 * If you don't need / can't get to every method, just implement
 * what you have time for.  The other methods will only fail if
 * you try to call them!
 *
 * For many adapters, this file is all you need.  For very complex adapters, you may need more flexiblity.
 * In any case, it's probably a good idea to start with one file and refactor only if necessary.
 * If you do go that route, it's conventional in Node to create a `./lib` directory for your private submodules
 * and load them at the top of the file with other dependencies.  e.g. var update = `require('./lib/update')`;
 */
module.exports = (function () {

  // Make data revive as they expect to be
  function dataReviver(key, value) {

    if (typeof value === 'string') {

      if( value.length === 0 ) {

        return null;
      } else {

        var a = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}(?:\.\d*)?)Z$/.exec(value);

        if ( a ) {

          return new Date( Date.UTC(+a[1], +a[2] - 1, +a[3], +a[4], +a[5], +a[6]) );
        }
      }
    }

    return value;
  }

  // Redefine restler json parser to add data reviver
  rest.parsers.json = function ( data, callback ) {
    
    if (data && data.length) {

      var parsedData;
      
      try {
      
        parsedData = JSON.parse( data, dataReviver );

      } catch ( err ) {

        err.message = 'Failed to parse JSON body: ' + err.message;
        callback(err, null);
      }

      if ( parsedData !== undefined ) {

        callback( null, parsedData );
      }
    } else {

      callback( null, null );
    }
  };

  // Apply it again for the auto matcher
  rest.parsers.auto.matchers['application/json'] = rest.parsers.json;

  // Rest Custom Error Object
  function RestError( message, meta ) {

    this.name = 'RestError';
    this.message = message || 'REST Error Message';
    this.meta = meta || {};
  }

  RestError.prototype = new Error();
  RestError.prototype.constructor = RestError;

  // Generate the URL of all requests
  function generateBaseUrl( config, collection ) {

    var host = config.host;

    if( config.protocol === 'http' && config.port !== 80 )
    {
      host += ':' + config.port;
    }
    else if( config.protocol === 'https' && config.port !== 443 )
    {
      host += ':' + config.port;
    }

    return config.protocol + '://' + host +
           config.prefix + '/' +
           collection + '/';
  }

  // Create the URL with options
  function populateUrlWithOptions( url, options, config ) {

    var optionsCount = 0,
        hasGroupBy = !!options.groupBy,
        limit = ( hasGroupBy ) ? config.groupByLimit : options.limit || 30;

    _.forEach(options, function( value, key ) {

      switch ( key ) {
        case 'groupBy': case 'skip':
        case 'min': case 'max': 
        case 'sum': case 'average': // These actions will be done after by WaterLine criteria
          break;

        case 'sort':

          if( value ) {
            if ( optionsCount === 0 ) url += '?';
            else url += '&';

            url += key + '=' + encodeURIComponent( JSON.stringify( value ) );

            optionsCount++;
          }
          break;

        default:
          if ( optionsCount === 0 ) url += '?';
          else url += '&';

          if ( !hasGroupBy && key === 'limit' ) {

            // Add limit in the url here, due to groupBy bug
            url += 'limit=' + limit;

          } else if( value ) {
          
            // Add default option in the url
            url += key + '=' + encodeURIComponent( JSON.stringify( value ) );
          }

          optionsCount++;
      }
    } );

    if ( url.slice( -1 ) === '?' ||
         url.slice( -1 ) === '&' ) {

      if ( hasGroupBy ) {

        // Add the limit
        url += 'limit=' + limit;

      } else {
     
        // Remove the last character if incorrect
        url = url.substring( 0, url.length - 1 );
      }

    // GroupBy add a bug with limit, so add a limit by default
    } else if ( hasGroupBy ) {

      if ( optionsCount === 0 ) url += '?';
      else url += '&';

      url += 'limit=' + limit;
    }

    return url;
  }

  // Define the error handler of requestss
  function errorHandler( onDone ) {

    return function( data, response ) {

      if ( response && ( response.statusCode === 200 || response.statusCode === 201 ) ) {

        if( typeof onDone === 'function' ) {

          return onDone( null, data );
        }
      } else if ( !response ) {

        return onDone( new RestError('No connection to host.'), null );
      } else {

        return onDone( new RestError('An error occured! [ http status: ' + response.statusCode + ' ]'), null );
      }
    };
  }

  // You'll want to maintain a reference to each connection
  // that gets registered with this adapter.
  var connections = {};

  // Set the adapter
  var adapter = {

    syncable: false,

    // The default delay for one request (in ms), slower can make some integrity problems with the server
    // higher will make requests slow
    delayRequest: 5,

    // Default configuration for connections
    defaults: {
      args: {

      },

      protocol: 'http',

      host: '',

      port: 80,

      prefix: '',

      groupByLimit: 1000,

      destroyIfEmptyLimit: 1000
    },

    /**
     *
     * This method runs when a model is initially registered
     * at server-start-time.  This is the only required method.
     *
     * @param  {[type]}   connection [description]
     * @param  {[type]}   collection [description]
     * @param  {Function} cb         [description]
     * @return {[type]}              [description]
     */
    registerConnection: function(connection, collections, cb) {

      if( !connection.identity ) return cb( new Error( 'Connection is missing an identity.' ) );
      if( connections[ connection.identity ] ) return cb( new Error( 'Connection is already registered.' ) );

      var config = this.defaults ? _.extend({}, this.defaults, connection) : connection;

      // Add in logic here to initialize connection
      connections[ connection.identity ] = {
        config: config,
        generateArgs: function( additional ) {

          return _.extend({}, this.config.args, additional);
        }
      };

      cb();
    },

    /**
     * Fired when a model is unregistered, typically when the server
     * is killed. Useful for tearing-down remaining open connections,
     * etc.
     *
     * @param  {Function} cb [description]
     * @return {[type]}      [description]
     */
    // Teardown a Connection
    teardown: function (conn, cb) {

      if ( typeof conn == 'function' ) {
        
        cb   = conn;
        conn = null;
      }
      
      if ( !conn ) {

        connections = {};
        return cb();
      }

      if( !connections[ conn ] ) return cb();
      
      delete connections[ conn ];

      cb();
    },

    // Return attributes
    describe: function (connection, collection, cb) {
			// Add in logic here to describe a collection (e.g. DESCRIBE TABLE logic)
      return cb();
    },

    /**
     *
     * REQUIRED method if integrating with a schemaful
     * (SQL-ish) database.
     *
     */
    define: function (connection, collection, definition, cb) {

      connections[ connection ].definition = definition;

      return cb();
    },

    /**
     *
     * REQUIRED method if integrating with a schemaful
     * (SQL-ish) database.
     *
     */
    drop: function (connection, collection, relations, cb) {
			// Add in logic here to delete a collection (e.g. DROP TABLE logic)
			return cb();
    },

    /**
     *
     * REQUIRED method if users expect to call Model.find(), Model.findOne(),
     * or related.
     *
     * You should implement this method to respond with an array of instances.
     * Waterline core will take care of supporting all the other different
     * find methods/usages.
     *
     */
    find: function ( connection, collection, options, cb ) {

      setTimeout( function() {

        // Initialize Data
        var conn   = connections[ connection ],
            url    = generateBaseUrl( conn.config, collection );

        // Generate the right URL
        url = populateUrlWithOptions( url, options, conn.config );

        // Send the request
        rest.get(
          url,
          conn.generateArgs()
        ).on('complete', errorHandler( function( err, data ) {

          if( err ) return cb( err );

          // Filter Data based on Options criteria
          var resultSet = waterlineCriteria( data, options );

          // Process Aggregate Options
          var aggregate = new Aggregate( options, resultSet.results );

          if( aggregate.error ) {

            return cb( aggregate.error );
          }

          return cb( null, aggregate.results );

        } ) );
      }, adapter.delayRequest );
    },

    create: function (connection, collection, values, cb) {

      setTimeout( function() {
        
        // Initialize Data
        var conn = connections[ connection ];

        rest.post(
          generateBaseUrl( conn.config, collection ),
          conn.generateArgs( {
            data: values
          } )
        ).on('complete', errorHandler( function( err, data ) {

          if( err ) return cb( err );

          return cb( null, data );
        } ) );
      }, adapter.delayRequest );
    },

    update: function (connection, collection, options, values, cb) {

      setTimeout( function() {
        
        // Initialize Data
        var conn = connections[ connection ],
            url  = generateBaseUrl( conn.config, collection );

        // Find all references for this options
        adapter.find(connection, collection, options, function( err, data ) {

          if( err ) return cb( err, null );

          var calls = [],
              returns = []; // Prepare returned values

          // Stack all request
          _.forEach( data, function( value ) {

            var id;

            if( value.id ) {

              id = value.id;
            }

            // Add the request to the stack
            calls.push( function( callback ) {

              // Send the update request
              rest.postJson(
                url + id,
                values,
                conn.generateArgs()
              ).on('complete', errorHandler( function( err, res ) {

                if( err ) return callback( err );

                returns.push( res );

                return callback( null );

              } ) );
            } );
          } );

          // Waterfall all the requests and return all results
          async.waterfall( calls, function ( err ) {

            return cb( null, returns ); 
          } );
        } );
      }, adapter.delayRequest );
    },

    destroy: function ( connection, collection, options, cb ) {

      setTimeout( function() {
        
        // Initialize Data
        var conn = connections[ connection ],
            url  = generateBaseUrl( conn.config, collection );

        // Due to a bug when suppressing all, a default limit is applied
        if( !options.where ) {

          options.limit = conn.config.destroyIfEmptyLimit;
        }

        // Find all references for this options
        adapter.find( connection, collection, options, function( err, data ) {

          if( err ) return cb( err, null );

          var calls = [];

          // Stack all request
          _.forEach( data, function( value ) {

            var id;

            if( value.id ) {

              id = value.id;
            }

            // Add the request to the stack
            calls.push( function( callback ) {

              // Send the delete request
              rest.del(
                url + id,
                conn.generateArgs()
              ).on('complete', errorHandler( function( err, data ) {

                return callback( err );

              } ) );
            } );
          } );

          // Waterfall all the requests and return all results
          async.waterfall( calls, function ( err ) {

            return cb( null, data ); 
          } );
        } );
      }, adapter.delayRequest );
    },

    custom: function ( connection, collection, options, cb ) {

      setTimeout( function() {
        
        // Initialize Data
        var conn        = connections[ connection ],
            method      = 'get',
            action      = options.action || '',
            url         = generateBaseUrl( conn.config, collection ) + action,
            requestData = options.data || {},
            request     = {};

        // Set the method from user
        if ( options.method &&
             typeof rest[ options.method.toLowerCase() ] === 'function' ) {

          method = options.method.toLowerCase();
        }

        // Check for request call on specific methods
        if ( method === 'json' || method === 'putJson' || method === 'postJson' ) {

          // on specific method
          request = rest[ method ]( url, requestData, conn.generateArgs() );
        } else {

          // for other method
          request = rest[ method ]( url, conn.generateArgs( {
            data: requestData
          } ) );
        }

        // If no error occured on request generation
        if ( request ) {

          // Handle response from the server
          request.on('complete', errorHandler( function( err, data ) {

            return cb( err, data );

          } ) );
        }
      }, adapter.delayRequest );
    }
  };

  // Expose adapter definition
  return adapter;
})();
