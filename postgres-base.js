/*
 * Copyright 2013 Jive Software
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */


var q = require('q');
    q.longStackSupport = true;
var jive = require('jive-sdk');
var PostgresClient = require('./postgres-client');

module.exports = function(serviceConfig) {
    var databaseUrl;
    var clientAcquireTimeoutMs;
    var dbPoolSize;

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Init

    // setup database url
    if (serviceConfig ) {
        databaseUrl = serviceConfig['databaseUrl'];
        clientAcquireTimeoutMs = serviceConfig['clientAcquireTimeoutMs'];
	dbPoolSize = serviceConfig['dbPoolSize'];
    }

    if ( !databaseUrl ) {
        throw new Error("Cannot initialize connection with empty database URL.");
    }

    if ( !clientAcquireTimeoutMs ) {
        // default to 15 second client acquisition timeout attempt
        clientAcquireTimeoutMs = 15 * 1000;
    }

    if ( !dbPoolSize ) {
        // default to 10 pooled connections
        dbPoolSize = 10;
    }

    jive.logger.info("Postgres connection pool ready.");
    jive.logger.info("Connect URL: ", databaseUrl);
    jive.logger.debug("clientAcquireTimeoutMs: ", clientAcquireTimeoutMs);
    jive.logger.info("dbPoolSize: ", dbPoolSize);

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Private

    // driver
    var postgres = require('pg');
    postgres.defaults.poolSize = dbPoolSize;

    jive.logger.debug('options.databaseUrl:', databaseUrl);
    jive.logger.debug('options.schema:',  serviceConfig['schema'] );
    jive.logger.debug('options.clientAcquireTimeoutMs:',  serviceConfig['clientAcquireTimeoutMs'] );
    jive.logger.debug('options.dbPoolSize:',  serviceConfig['dbPoolSize'] );

//    var t = new Date().getTime();

    var badErrorCodeClasses = ['08','53','54','55','57','58','F0','P0','XX'];

    function requestClient(clientID, deferred) {

        var clientCheckoutCountdownInterval = setTimeout(
            function() {
                // kill the client request if takes too long
                deferred.reject(new Error("Could not fetch a client from postgres, aborting."));
                clientID = null;
            }, clientAcquireTimeoutMs); // should be able to retrieve a client in 15 seconds; otherwise timeout

        // get a pg client from the connection pool
        postgres.connect(databaseUrl, function(err, client, done) {
            // clear the error-throwing timeout since we got a response from postgres
            clearTimeout(clientCheckoutCountdownInterval);

            if ( err ) {
                deferred.reject(new Error(err));
                return;
            }

            if ( !clientID ) {
                // the countdown timer killed it; release this client
                done();
                return q.reject("Connection timed out; client acquired late was released and is not available.");
            }

            var handleError = function(err) {
                // no error occurred, continue with the request
                if(!err) return false;

                var errClass = err.code.substr(0, 2);
                if ( badErrorCodeClasses.indexOf(errClass) > -1 ) {
                    // An SERIOUS error occurred, remove the client from the connection pool.
                    // A truthy value passed to done will remove the connection from the pool
                    // instead of simply returning it to be reused.
                    // In this case, if we have successfully received a client (truthy)
                    // then it will be removed from the pool.
                    done(client);
                }
                return true;
            };

            var postgresClient = new PostgresClient(client, done, handleError);
            deferred.resolve(postgresClient);
        });
    }

    function getClient() {
        var deferred = q.defer();

//        if ( new Date().getTime() - t > 10000 && new Date().getTime() % 4 == 0 ) {
//            return q.reject(new Error("Failed to get a client"));
//        }

        var clientID = jive.util.guid();
        requestClient(clientID, deferred);

        return deferred.promise;
    }

    function query(sql) {
       return getClient().then( function(client) {
           return client.query(sql);
       });
    }

    function destroy() {
        var p = q.defer();
        return p.promise;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Public API

    var postgresObj = {
        query : query,
        destroy: destroy,
        getClient : getClient
    };

    return postgresObj;
};
