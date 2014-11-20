/**
 * Module Dependencies
 */

var _     = require('lodash'),
    async = require('async'),
    rest  = require('restler');

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

rest.parsers.auto.matchers['application/json'] = rest.parsers.json;

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


  // You'll want to maintain a reference to each connection
  // that gets registered with this adapter.
  var connections = {};

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

  function populateUrlWithOptions( url, options ) {

    var optionsCount = 0;

    _.forEach(options, function( value, key ) {

      if ( optionsCount === 0 ) url += '?';
      else url += '&';

      // Add option in the url
      url += key + '=' + JSON.stringify( value );

      optionsCount++;
    } );

    return url;
  }

  // You may also want to store additional, private data
  // per-connection (esp. if your data store uses persistent
  // connections).
  //
  // Keep in mind that models can be configured to use different databases
  // within the same app, at the same time.
  //
  // i.e. if you're writing a MariaDB adapter, you should be aware that one
  // model might be configured as `host="localhost"` and another might be using
  // `host="foo.com"` at the same time.  Same thing goes for user, database,
  // password, or any other config.
  //
  // You don't have to support this feature right off the bat in your
  // adapter, but it ought to get done eventually.
  //

  var adapter = {

    // Set to true if this adapter supports (or requires) things like data types, validations, keys, etc.
    // If true, the schema for models using this adapter will be automatically synced when the server starts.
    // Not terribly relevant if your data store is not SQL/schemaful.
    //
    // If setting syncable, you should consider the migrate option,
    // which allows you to set how the sync will be performed.
    // It can be overridden globally in an app (config/adapters.js)
    // and on a per-model basis.
    //
    // IMPORTANT:
    // `migrate` is not a production data migration solution!
    // In production, always use `migrate: safe`
    //
    // drop   => Drop schema and data, then recreate it
    // alter  => Drop/add columns as necessary.
    // safe   => Don't change anything (good for production DBs)
    //
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

          return _.extend({}, this.config.args, additional)
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
			// Add in logic here to create a collection (e.g. CREATE TABLE logic)
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

      var conn = connections[ connection ],
          url  = generateBaseUrl( conn.config, collection );

      url = populateUrlWithOptions( url, options );

      rest.get(
        url,
        conn.generateArgs()
      ).on('complete', function( data, response ) {

        if ( response.statusCode === 200 ) {

          return cb( null, data );
        } else {

          return cb( new Error('An error occured! [ http status: ' + response.statusCode + ' ]') );
        }
      } );
    },

    create: function (connection, collection, values, cb) {

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

          return cb( new Error('An error occured! [ http status: ' + response.statusCode + ' ]') );
        }
      } );
    },

    update: function (connection, collection, options, values, cb) {

      var conn = connections[ connection ];

      url = populateUrlWithOptions( url, options );

      rest.post(
        generateBaseUrl( conn.config, collection ),
        conn.generateArgs( {
          data: values
        } )
      ).on('complete', function( data, response ) {

        if ( response.statusCode === 200 ) {

          return cb( null, data );
        } else {

          return cb( new Error('An error occured! [ http status: ' + response.statusCode + ' ]') );
        }
      } );
    },

    destroy: function (connection, collection, options, values, cb) {

      if( typeof values === 'function' ) {

        cb = values;
        values = {};
      }

      var conn = connections[ connection ],
          url  = generateBaseUrl( conn.config, collection );

      this.find(connection, collection, options, function( err, data ) {

        if( err ) return cb( err, null );

        var calls = [];

        _.forEach( data, function( value ) {

          var id;

          if( value.id ) {
            id = value.id;
          }

          calls.push( function( callback ) {

            rest.del(
              url + id,
              conn.generateArgs()
            ).on('complete', function( res, response ) {

              if ( response.statusCode === 200 ) {

                return callback( null );
              } else {

                return callback( new Error('An error occured! [ http status: ' + response.statusCode + ' ]') );
              }
            } );
          } );
        } );

        async.waterfall( calls, function ( err ) {
          return cb( null, data ); 
        } );
      } );
    }

    /*

    // Custom methods defined here will be available on all models
    // which are hooked up to this adapter:
    //
    // e.g.:
    //
    foo: function (collectionName, options, cb) {
      return cb(null,"ok");
    },
    bar: function (collectionName, options, cb) {
      if (!options.jello) return cb("Failure!");
      else return cb();
      destroy: function (connection, collection, options, values, cb) {
       return cb();
     }

    // So if you have three models:
    // Tiger, Sparrow, and User
    // 2 of which (Tiger and Sparrow) implement this custom adapter,
    // then you'll be able to access:
    //
    // Tiger.foo(...)
    // Tiger.bar(...)
    // Sparrow.foo(...)
    // Sparrow.bar(...)


    // Example success usage:
    //
    // (notice how the first argument goes away:)
    Tiger.foo({}, function (err, result) {
      if (err) return console.error(err);
      else console.log(result);

      // outputs: ok
    });

    // Example error usage:
    //
    // (notice how the first argument goes away:)
    Sparrow.bar({test: 'yes'}, function (err, result){
      if (err) console.error(err);
      else console.log(result);

      // outputs: Failure!
    })




    */




  };


  // Expose adapter definition
  return adapter;

})();

