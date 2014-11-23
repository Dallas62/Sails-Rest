/**
 * Module Dependencies
 */

var _     = require('lodash'),
    async = require('async'),
    rest  = require('restler');

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

          return new Date(Date.UTC(+a[1], +a[2] - 1, +a[3], +a[4], +a[5], +a[6]));
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

    if( config.method === 'http' && config.port !== 80 )
    {
      host += ':' + config.port;
    }
    else if( config.method === 'https' && config.port !== 443 )
    {
      host += ':' + config.port;
    }

    return config.method + '://' + host +
           config.prefix + '/' +
           collection + '/';
  }

  // Prevent floating point bug, round it at 12
  function roundValue( value ) {
    var coef = 1000000000000;

    return Math.round( value * coef ) / coef;
  }

  // Filter request to add a filter
  function extractSpecialQueries( options ) {

    var filter;

    if ( options.sum || options.average || options.min || options.max ) {

      var N       = 1,
          sum     = _.clone( options.sum ) || [],
          average = _.clone( options.average ) || [],
          min     = _.clone( options.min ) || [],
          max     = _.clone( options.max ) || [],
          allIn   = _.union( sum, average, min, max );

      delete options.sum;
      delete options.average;
      delete options.min;
      delete options.max;

      filter = function( result, value, key ) {

        _.forEach( allIn, function( column ) {
          
          // Define default value first if sum or average
          if ( !result[ column ] &&
            (
               _.contains( sum, column ) ||
               _.contains( average, column )
            )
          ) {
          
            result[ column ] = 0;
          }

          // Apply sum if in the list
          if ( _.contains( sum, column ) ) {

            result[ column ] += value[ column ];
          }

          // Apply average if in the list
          if ( _.contains( average, column ) ) {

            result[ column ] = roundValue( result[ column ] * ( N - 1 ) / N + value[ column ] / N );
          }

          // Apply min if in the list
          if ( _.contains( min, column ) ) {

            result[ column ] = Math.min(
              result[ column ] || Number.MAX_VALUE,
              value[ column ]
            );
          }

          // Apply max if in the list
          if ( _.contains( max, column ) ) {

            result[ column ] = Math.max(
              result[ column ] || Number.MIN_VALUE,
              value[ column ]
            );
          }
        } );

        N++;
        return result;
      };
    }

    return filter;
  }

  // Create the URL with options
  function populateUrlWithOptions( url, options ) {

    var optionsCount = 0;

    _.forEach(options, function( value, key ) {

      if ( optionsCount === 0 ) url += '?';
      else url += '&';

      switch ( key ) {
        case 'sort':

          var orderCount = 0;
      
          url += 'sort=';

          // Format sort option
          if( _.isString( value ) ) {

            url += encodeURIComponent( value );
          } else {

            _.forEach( value, function ( order, column ) {

              if ( orderCount !== 0 ) url += encodeURIComponent( ' ' );

              orderCount++;

              if ( _.isNumber( order ) ) {
                
                url += encodeURIComponent( column + ' ' + ( ( order > 0 ) ? 'ASC' : 'DESC' ) );
              } else {

                url += encodeURIComponent( column + ' ' + order );
              }
            } );
          }

          break;

        default:

          // Add default option in the url
          url += key + '=' + encodeURIComponent( JSON.stringify( value ) );
      }

      optionsCount++;
    } );

    console.log( url );

    return url;
  }

  // You'll want to maintain a reference to each connection
  // that gets registered with this adapter.
  var connections = {};

  // Set the adapter
  var adapter = {

    syncable: false,

    // Default configuration for connections
    defaults: {
      args: {

      },

      method: 'http',

      host: '',

      port: 80,

      prefix: ''
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
    find: function (connection, collection, options, cb) {

      // Initialize Data
      var conn   = connections[ connection ],
          url    = generateBaseUrl( conn.config, collection ),
          filter = extractSpecialQueries( options );

      // Generate the right URL
      url = populateUrlWithOptions( url, options );

      // Send the request
      rest.get(
        url,
        conn.generateArgs()
      ).on('complete', function( data, response ) {

        if ( response.statusCode === 200 ) {

          if ( !filter ) {

            return cb( null, data );
          } else {

            var filtered = [ _.reduce( data, filter, {}) ];

            return cb( null, filtered );
          }
        } else {

          return cb( new RestError('An error occured! [ http status: ' + response.statusCode + ' ]') );
        }
      } );
    },

    create: function (connection, collection, values, cb) {

      // Initialize Data
      var conn = connections[ connection ];

      rest.post(
        generateBaseUrl( conn.config, collection ),
        conn.generateArgs( {
          data: values
        } )
      ).on('complete', function( data, response ) {

        if ( response.statusCode === 200 ) {

          return cb( null, data );
        } else {

          return cb( new RestError('An error occured! [ http status: ' + response.statusCode + ' ]') );
        }
      } );
    },

    update: function (connection, collection, options, values, cb) {

      // Initialize Data
      var conn = connections[ connection ],
          url  = generateBaseUrl( conn.config, collection );

      // Find all references for this options
      this.find(connection, collection, options, function( err, data ) {

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
            ).on('complete', function( res, response ) {

              if ( response.statusCode === 200 ) {

                returns.push( res );

                return callback( null );
              } else {

                return callback( new RestError('An error occured! [ http status: ' + response.statusCode + ' ]') );
              }
            } );
          } );
        } );

        // Waterfall all the requests and return all results
        async.waterfall( calls, function ( err ) {

          return cb( null, returns ); 
        } );
      } );
    },

    destroy: function (connection, collection, options, values, cb) {

      // Initialize Data
      if( typeof values === 'function' ) {

        cb = values;
        values = {};
      }

      var conn = connections[ connection ],
          url  = generateBaseUrl( conn.config, collection );

      // Find all references for this options
      this.find(connection, collection, options, function( err, data ) {

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
            ).on('complete', function( res, response ) {

              if ( response.statusCode === 200 ) {

                return callback( null );
              } else {

                return callback( new RestError('An error occured! [ http status: ' + response.statusCode + ' ]') );
              }
            } );
          } );
        } );

        // Waterfall all the requests and return all results
        async.waterfall( calls, function ( err ) {

          return cb( null, data ); 
        } );
      } );
    }
  };

  // Expose adapter definition
  return adapter;
})();
