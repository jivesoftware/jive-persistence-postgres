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
var jive = require('jive-sdk');
var flat = require('flat');
var ArrayStream = require('stream-array');
var SchemaSyncer = require('./postgres-schema-syncer');

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

    q.longStackSupport = true;

    // driver
    var postgres = require('./postgres-base');
    var db = new postgres( {
        databaseUrl : databaseUrl
    });
    var schemaSyncer = new SchemaSyncer(db, serviceConfig['schema']);

    jive.logger.debug('options.databaseUrl:', databaseUrl);
    jive.logger.debug('options.schema:',  serviceConfig['schema'] );

    function isValue(value) {
        return value || typeof value === 'number';
    }

    function sanitize(key) {
        return key.replace('.', '_');
    }

    function hydrate(row) {
        var toUnflatten = {};
        var needFlatten;
        for (var dataKey in row) {

            if (row.hasOwnProperty(dataKey)) {
                var value = row[dataKey];
                if (isValue(value) ) {
                    if ( value.indexOf && value.indexOf('<__@> ') == 0 ) {
                        value = value.split('<__@> ')[1];
                        value = JSON.parse(value);
                        needFlatten = true;
                    }
                    toUnflatten[dataKey] = value;
                }
            }
        }
        var obj = needFlatten ? flat.unflatten(toUnflatten, {'delimiter': '_'}) : toUnflatten;
        delete obj[""];
        return obj;
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

    function buildQueryArguments(collectionID, data, key) {
        var keys = [], values = [], sanitized = {}, dataToSave = {};
        var collectionSchema = schemaSyncer.getTableSchema(collectionID);

        if (typeof data === 'object') {
            for (var k in collectionSchema) {
                if (!collectionSchema.hasOwnProperty(k)) {
                    continue;
                }
                var keyParts = k !== '_id' ? k.split('_') : [k];
                var entry = data;
                var notFound = false;
                for (var kp in keyParts) {
                    if (entry) {
                        entry = entry[ keyParts[kp]];
                    } else {
                        notFound = true;
                        break;
                    }
                }

                if (!notFound) {
                    dataToSave[k] = typeof entry === 'object' ? '<__@> ' + JSON.stringify(entry, null, 4) : entry;
                }
            }
        } else {
            dataToSave[key] = data;
            dataToSave['_id'] = '' + key;
        }

        for (var dataKey in dataToSave) {
            if (dataToSave.hasOwnProperty(dataKey)) {
                var value = dataToSave[dataKey];
                if (dataKey.indexOf('.') > -1) {
                    var originalKey = dataKey;
                    dataKey = sanitize(dataKey);
                    sanitized[dataKey] = originalKey;
                }
                if (isValue(value)) {
                    keys.push("\"" + dataKey + "\"");
                    if (typeof value == 'object') {
                        value = JSON.stringify(value);
                    }
                    values.push(isValue(value) ? "'" + value + "'" : 'null');
                }
            }
        }

        return {
            keys : keys,
            values : values
        };
    }

    var postgresObj = {

        /**
         * Save the provided data in a named collection, and return promise
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
                var structure = buildQueryArguments(collectionID, data, key);
                var values = structure.values;
                var keys = structure.keys;
                if (values.length < 1) {
                    throw new Error("cannot insert empty data");
                }

                var sql = "update \"" + collectionID + "\" set";
                for ( var i = 0 ; i < keys.length; i++ ) {
                    sql += " " + keys[i] + "= " + values[i]
                        + ( ( i < keys.length - 1 ) ? "," : "");
                }
                sql += " where _id='" + key + "'";

                return query(sql).then(
                    function(r) {
                        if (r.rowCount < 1 ) {
                            return q.resolve(false);
                        } else {
                            return q.resolve(true);
                        }
                    },

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
                    var structure = buildQueryArguments(collectionID, data, key);
                    var values = structure.values;
                    var keys = structure.keys;
                    if (values.length < 1) {
                        throw new Error("cannot insert empty data");
                    }

                    var sql = "insert into \"" + collectionID + "\" ( " + keys.join(',') + " ) " +
                        "values ( " + values.join(',') + ")";

                    return query(sql).then(
                        function(r) {
                            if (r.rowCount < 1 ) {
                                throw new Error( "failed to insert");
                            }
                            return q.resolve(data);
                        },

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
                            //
                            deferred.resolve(r);
                        }
                    );
                }
            )
                .catch( function(e) {
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
         */
        find: function( collectionID, criteria, cursor, limit) {
            collectionID = collectionID.toLowerCase();

            var deferred = q.defer();

            schemaSyncer.prepCollection(collectionID)
                .then( function() {
                    return expandIfNecessary(collectionID, schemaSyncer.getTableSchema(collectionID), null, criteria);
                }).then( function() {
                    var where = [];

                    if ( criteria ) {
                        for ( var dataKey in criteria ) {

                            if ( criteria.hasOwnProperty(dataKey) ) {
                                var original = dataKey;
                                dataKey = sanitize(dataKey);

                                var tableSchema = schemaSyncer.getTableSchema(collectionID);
                                if ( tableSchema && tableSchema[dataKey]) {
                                    var value = criteria[original];

                                    if ( typeof value == 'object') {
                                        var $gt = value['$gt'];
                                        var $gte = value['$gte'];
                                        var $lt = value['$lt'];
                                        var $lte = value['$lte'];
                                        var $in = value['$in'];

                                        dataKey = "\"" + dataKey + "\"";

                                        var subClauses = [];
                                        if ( $gt ) {
                                            subClauses.push( dataKey + " > '" + $gt + "'");
                                        }

                                        if ( $gte ) {
                                            subClauses.push( dataKey + " >= '" + $gte  + "'");
                                        }

                                        if ( $lt ) {
                                            subClauses.push( dataKey + " < '" + $lt + "'" );
                                        }

                                        if ( $lte ) {
                                            subClauses.push( dataKey + " <= '" + $lte + "'" );
                                        }

                                        if ( $in ) {
                                            var ins = [];
                                            $in.forEach( function(i) {
                                                ins.push("'" + i + "'");
                                            });
                                            subClauses.push( dataKey + " in (" + ins.join(',') + ")" );

                                        }
                                        where.push( "(" + subClauses.join(' AND ') + ")");

                                    } else {
                                        dataKey = "\"" + dataKey + "\"";
                                        var whereClause = dataKey + " = '" + value + "'";
                                        where.push(whereClause);
                                    }
                                } else {
                                    deferred.reject(new Error(collectionID + "." + dataKey + " does not exist"));
                                    return;
                                }
                            }
                        }
                    }

                    var sql = "select * from \"" + collectionID + "\" ";
                    if ( where.length > 0 ) {
                        sql += "where " + where.join(' AND ');
                    }
                    if ( limit ) {
                        sql += " limit " + limit;
                    }
                    query(sql).then( function(r) {
                        var results = [];

                        if ( !r || r.rowCount < 1 ) {
                            deferred.resolve(results);
                            return;
                        }

                        // build a json structure from the results, based on '_' delimiter
                        if (r.rows['indexOf']) {
                            r.rows.forEach( function(row) {
                                var obj = hydrate(row);
                                results.push(obj);
                            });
                        } else {
                            results.push(hydrate(r.rows));
                        }

                        if ( !cursor ) {
                            deferred.resolve( results );
                        } else {
                            var stream = createStreamFrom(results);
                            deferred.resolve(stream );
                        }
                    }, function(e) {
                        jive.logger.error(e.stack);
                        deferred.reject(e);
                    });
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
         * @param collectionID
         * @param key
         */
        remove : function( collectionID, key ) {
            collectionID = collectionID.toLowerCase();

            var deferred = q.defer();

            startTx()
            .then( function() {
                if ( key ) {
                    return query("delete from \"" + collectionID + "\" where _id = '" + key + "'");
                } else {
                    return query("delete from \"" + collectionID + "\"");
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
