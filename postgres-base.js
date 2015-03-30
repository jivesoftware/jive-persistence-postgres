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

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Init

    // setup database url
    if (serviceConfig ) {
        databaseUrl = serviceConfig['databaseUrl'];
    }

    if ( !databaseUrl ) {
        throw new Error("Cannot initialize connection with empty database URL.");
    }

    jive.logger.info("Postgres connection pool ready.");
    jive.logger.info("Connected to", databaseUrl);

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Private

    // driver
    var postgres = require('pg');

    jive.logger.debug('options.databaseUrl:', databaseUrl);
    jive.logger.debug('options.schema:',  serviceConfig['schema'] );

    function getClient() {
        var deferred = q.defer();

        // get a pg client from the connection pool
        postgres.connect(databaseUrl, function(err, client, done) {
            if ( err ) {
                deferred.reject(new Error(err).stack);
                return;
            }
            var handleError = function(err) {
                // no error occurred, continue with the request
                if(!err) return false;

                // An error occurred, remove the client from the connection pool.
                // A truthy value passed to done will remove the connection from the pool
                // instead of simply returning it to be reused.
                // In this case, if we have successfully received a client (truthy)
                // then it will be removed from the pool.
                done(client);
                return true;
            };

            var postgresClient = new PostgresClient(client, done, handleError);

            setTimeout( function() {
                if ( !postgresClient.released ) {
                    // kill the client if takes too long
                    jive.logger.error(new Error("Client has not yet been released! Closing connection, returning it to the pool.").stack);
                    handleError(client);
                }
            }, 10000);

            deferred.resolve(postgresClient);
        });
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
