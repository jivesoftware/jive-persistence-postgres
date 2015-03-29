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
var ArrayStream = require('stream-array');
var SchemaSyncer = require('./postgres-schema-syncer');
var SqlAdaptor = require('./postgres-sql-adaptor');

module.exports = function(serviceConfig) {
    var databaseUrl;

    // setup database url
    if (serviceConfig ) {
        databaseUrl = serviceConfig['databaseUrl'];
    }

    if ( !databaseUrl ) {
        databaseUrl = 'pg://postgres:postgres@localhost:5432/mydb';
    }

    jive.logger.info("*******************");
    jive.logger.info("Postgres configured");
    jive.logger.info("*******************");

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Private

    // driver
    var postgres = require('./postgres-base');
    var db = new postgres( {
        databaseUrl : databaseUrl
    });
    var schemaSyncer = new SchemaSyncer(db, serviceConfig['schema']);
    var sqlAdaptor = new SqlAdaptor(schemaSyncer);

    jive.logger.debug('options.databaseUrl:', databaseUrl);
    jive.logger.debug('options.schema:',  serviceConfig['schema'] );

    function isValue(value) {
        return value || typeof value === 'number';
    }

    function query(sql) {
        return db.query(sql);
    }

    function startTx() {
        return db.startTx();
    }

    function commitTx() {
        return db.commitTx();
    }

    function rollbackTx(e) {
        if ( e ) {
            jive.logger.error(e.stack);
        }
        return db.rollbackTx();
    }

    function expandIfNecessary(collectionID, collectionSchema, key, data ) {
        return schemaSyncer.expandIfNecessary(collectionID, collectionSchema, key, data);
    }

    function throwError(detail) {
        var error = new Error(detail);
        jive.logger.error(error.stack);
        throw error;
    }

    function createStreamFrom(results) {
        var stream = ArrayStream(results);
        // graft next method
        stream.nextCtr = 0;
        stream.fullCollection = results;
        stream.next = function (processorFunction) {
            if (!processorFunction) {
                return null;
            }
            this.nextCtr++;
            if (this.nextCtr > this.fullCollection.length - 1) {
                processorFunction(null, null);
            } else {
                processorFunction(null, this.fullCollection[this.nextCtr]);
            }
        };
        return stream;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Public

    var postgresObj = {

        /**
         * Save the provided data in a named collection (updating if exists; otherwise, inserts), and return promise.
         *  Is transactional - will rollback on error.
         * @param collectionID
         * @param key
         * @param data
         */
        save : function( collectionID, key, data) {
            collectionID = collectionID.toLowerCase();
            var deferred = q.defer();

            schemaSyncer.prepCollection(collectionID).then( function() {
                if ( typeof data !== "object" ) {
                    // the data is a primitive
                    // therefore its a table with a single column, whose value is that primitive
                } else if ( data && !data['_id'] ) {
                    // the data is an object
                    data._id = key;
                }
            }).then( function() {
                return expandIfNecessary(collectionID, schemaSyncer.getTableSchema(collectionID), key, data);
            }).then( function() {
                return startTx();
            }).then( function() {
                // try to update first
                var sql = sqlAdaptor.createUpdateSQL(collectionID, data, key);
                return query(sql).then(
                    // success
                    function(r) {
                        return q.resolve(r.rowCount >= 1);
                    },

                    // error
                    function(e) {
                        return rollbackTx(e).finally( function() {
                            deferred.reject(e);
                        });
                    }
                );
            }).then( function(updated) {
                if (updated ) {
                    // we're done
                    return q.resolve(data);
                } else {
                    // otherwise do insert
                    var sql = sqlAdaptor.createInsertSQL(collectionID, data, key);
                    return query(sql).then(
                        // success
                        function(r) {
                            if (r.rowCount < 1 ) {
                                throwError("failed to insert");
                            }
                            return q.resolve(data);
                        },

                        // error
                        function(e) {
                            return rollbackTx(e).finally( function() {
                                deferred.reject(e);
                            });
                        }
                    );
                }
            })
            .then(
                function(r) {
                    return commitTx().then(
                        function() {
                            deferred.resolve(r);
                        }
                    );
                }
            ).catch( function(e) {
                return rollbackTx(e).finally( function() {
                    deferred.reject(e);
                });
            });

            return deferred.promise;
        },

        /**
         * Retrieve a piece of data from a named collection, based on the criteria, return promise
         * with an array of the results when done.
         * @param collectionID
         * @param criteria
         * @param cursor if true, then returned item is a cursor; otherwise its a concrete collection (array) of items
         * @param limit optional
         */
        find: function( collectionID, criteria, cursor, limit) {

            collectionID = collectionID.toLowerCase();

            var deferred = q.defer();

            schemaSyncer.prepCollection(collectionID)
                .then( function() {
                    return expandIfNecessary(collectionID, schemaSyncer.getTableSchema(collectionID), null, criteria);
                }).then( function() {
                    var sql = sqlAdaptor.createSelectSQL(collectionID, criteria, limit);
                    query(sql).then(
                        // success
                        function(r) {
                            if ( !r || r.rowCount < 1 ) {
                                // if no results, return empty array
                                deferred.resolve([]);
                                return;
                            }

                            var results = sqlAdaptor.hydrateResults(r);

                            if ( !cursor ) {
                                deferred.resolve( results );
                            } else {
                                var stream = createStreamFrom(results);
                                deferred.resolve(stream );
                            }
                        },

                        // error
                        function(e) {
                            jive.logger.error(e.stack);
                            deferred.reject(e);
                        }
                    );
                });

            return deferred.promise;
        },

        /**
         * Retrieve a piece of data from a named collection whose key is the one provided.
         * @param collectionID
         * @param key
         */
        findByID: function( collectionID, key ) {
            collectionID = collectionID.toLowerCase();

            var deferred = q.defer();
            schemaSyncer.prepCollection(collectionID).then( function() {
                postgresObj.find( collectionID, {'_id': key}, false, 1 ).then( function(r) {
                    if ( r && r.length > 0 ) {
                        var firstElement = r[0];
                        if ( isValue(firstElement[key]) ) {
                            var value = firstElement[key];
                            deferred.resolve(value);
                        } else {
                            deferred.resolve(firstElement);
                        }
                    }
                    return deferred.resolve(null);
                }, function(e) {
                    return q.reject(e);
                });
            });

            return deferred.promise;
        },

        /**
         * Remove a piece of data from a name collection, based to the provided key, return promise
         * containing removed items when done.
         * If no key is provided, all the data from the collection is removed.
         * Is transactional - will rollback on error.
         * @param collectionID
         * @param key
         */
        remove : function( collectionID, key ) {
            collectionID = collectionID.toLowerCase();

            var deferred = q.defer();

            startTx()
            .then( function() {
                var sql = sqlAdaptor.createDeleteSQL(collectionID, key);
                return query(sql);
            })
            .then(
                function(r) {
                    return commitTx().then(
                        function() {
                            deferred.resolve(r);
                        }
                    );
                }
            ).catch( function(e) {
                return rollbackTx(e).finally( function() {
                    deferred.reject(e);
                });
            });

            return deferred.promise;
        },

        close: function() {
            return q.resolve();
        },

        destroy: function() {
            var p = q.defer();
            return p.promise;
        },

        /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // specific to postgres connector

        query: function(sql) {
            return query(sql);
        },

        startTx : startTx,

        commitTx : commitTx,

        rollbackTx : rollbackTx,

        init: function(collectionID) {
            return schemaSyncer.prepCollection(collectionID);
        },

        sync: function( toSync, dropIfExists ) {
            return schemaSyncer.syncCollections(toSync, dropIfExists);
        }

    };

    return postgresObj;
};
