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
    var postgresDialect = require('sql-ddl-sync/lib/Dialects/postgresql');

    var schema = {};
    var toSync = {};
    var analyzed = {};
    if ( serviceConfig['schema'] ) {
        toSync = serviceConfig['schema'];
        if ( toSync ) {
            for ( var k in toSync ) {
                if (toSync.hasOwnProperty(k) ) {
                    var value = toSync[k];
                    delete toSync[k];
                    toSync[k.toLowerCase()] = value;
                }
            }
        }
    }

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

    function tableExists(table) {
        return query("select * from pg_tables where tablename='" + table + "'").then( function(r) {
            return r && r.rowCount > 0;
        }, function(e) {
            return q.reject(e);
        });
    }

    function dropTable(table) {
        return query("drop table if exists \"" + table + "\"").then( function(r) {
            return r;
        }, function(e) {
            return q.reject(e);
        });
    }

    function registerTable(collectionID, tableAttrs) {
        // sanitize column names
        for (var key in tableAttrs) {
            if (tableAttrs.hasOwnProperty(key)) {
                var value = tableAttrs[key];
                if (key.indexOf('.') > -1) {
                    delete tableAttrs[key];
                    key = sanitize(key);
                    tableAttrs[key] = value;
                }
                schema[collectionID] = tableAttrs;
            }
        }
    }

    function syncTable( table, dropIfExists, force ) {
        var p = q.defer();

        var collectionID = table['tableName'];
        collectionID = collectionID.replace('"','');
        collectionID = collectionID.toLowerCase();

        var tableAttrs = table['attrs'];
        if ( !tableAttrs['_id'] ) {
            tableAttrs['_id'] = { type: "text", required: true, index: true, unqiue: true };
        }

        registerTable(collectionID, tableAttrs);

        q.resolve().then( function() {
            // start a transaction
            return q.resolve();
        }).then( function() {
            // check if table exists
            return tableExists(collectionID);
        }).then( function(exists) {
            if ( (exists && !force) && !dropIfExists ) {
                // nothing to do:
                // - the table exists, and we're not forcing any changes
                // - we are not dropping the table
                return q.resolve({
                    exists : exists
                });
            } else {
                // a sync operation is required; grab a client
                return db.getClient().then( function(client) {
                    return {
                        client : client,
                        exists : exists
                    }
                });
            }
        }).then( function(r) {
            var dbClient = r.client;
            var exists = r.exists;
            var syncDeferred = q.defer();

            if ( dbClient && (!exists || force) ) {

                // if we're here, then the table exists; only run the sync if its forced
                var Sync = require("sql-ddl-sync").Sync;
                var sync = new Sync({
                    suppressColumnDrop: true,
                    dialect : "postgresql",
                    db      : dbClient.rawClient(),
                    debug   : function (text) {
                        jive.logger.info("> %s", text);
                    }
                });
                sync.defineCollection(collectionID, tableAttrs);

                sync.sync(function (err) {
                    if (err) {
                        jive.logger.error("> Sync Error", err);
                        dbClient.release();
                        throw new Error(err);
                    } else {
                        jive.logger.info("> Sync Done", collectionID );
                        dbClient.release();
                        delete toSync[collectionID];
                        syncDeferred.resolve();
                    }
                });

            } else if (dropIfExists ) {
                return dropTable(collectionID).then( function() {
                    return syncTable(table, false).then( function() {
                        if ( dbClient ) {
                            dbClient.release();
                        }
                        syncDeferred.resolve();
                    }, function(e) {
                        if ( dbClient ) {
                            dbClient.release();
                        }
                        throw new Error(e);
                    })
                });
            } else {
                jive.logger.debug("table already exists");
                if ( dbClient ) {
                    dbClient.release();
                }
                syncDeferred.resolve();
            }
            return syncDeferred.promise;
        }).then( function() {
                p.resolve();
            }, function(e) {
                jive.logger.error(e.stack);
                p.reject(e);
            }
        ).catch( function(e) {
                jive.logger.error(e.stack);
                p.reject(e);
            });

        return p.promise;
    }

    function expandIfNecessary(collectionID, collectionSchema, key, data ) {
        var requireSync;
        var lazyCreateCollection = true; // todo -- parameterize

        if ( !collectionSchema ) {
            // collection doesn't exist
            if ( lazyCreateCollection ) {
                collectionSchema = {};
                schema[collectionID] = collectionSchema;
                requireSync = true;
            } else {
                // don't create the collection if lazy create is not allowed
                return q.resolve();
            }
        }

        if ( typeof data === 'object' ) {
            // data is an object
            // unpack it
            for ( var dataKey in data ) {
                if ( !data.hasOwnProperty(dataKey) ) {
                    continue;
                }

                dataKey = dataKey.replace('.', '_');

                if ( !collectionSchema[dataKey] ) {
                    // collection schema doesn't have the attribute
                    if ( lazyCreateCollection ) {
                        // if lazy collection is enabled, then add it and stimulate a sync
                        // mark it as efinixpandable, since it was dynamically created
                        collectionSchema[dataKey] = { type: "text", required: false, expandable: true };
                        requireSync = true;
                    } else {
                        // lazy collection is not enabled, therefore don't add it to schema (or expanding)
                        // and avoid syncing
                        continue;
                    }
                }

                // the attribute is in the collection schema, its expandable if its an object and if its marked expandable
                var dataValue = data[dataKey];
                var expandable = collectionSchema[dataKey].expandable && typeof dataValue === 'object';
                if ( !expandable ) {
                    // if its not an expandable, then leave it alone
                    continue;
                }

                // its an expandable field: expand it (eg. make new columns)
                var flattened = flat.flatten(dataValue, {'delimiter': '_'});
                for ( var k in flattened ) {
                    if ( flattened.hasOwnProperty(k)) {
                        if (k.indexOf('$lt')  > -1 || k.indexOf('$gt')  > -1
                            || k.indexOf('$lte') > -1 || k.indexOf('$gte') > -1 || k.indexOf('$in') > -1 ) {
                            continue;
                        }

                        if ( !collectionSchema[dataKey + '_' + k] ) {
                            collectionSchema[dataKey + '_' + k] = { type: "text", required: false, expandable: true };
                            requireSync = true;
                        }
                    }
                }
            }
        }
        else {
            if ( key && !collectionSchema[key] ) {
                // collection schema doesn't have the attribute
                if ( lazyCreateCollection ) {
                    // if lazy collection is enabled, then add it and stimulate a sync
                    // mark it as expandable, since it was dynamically created
                    // introspect its type based on the value
                    if ( typeof data !== 'function' ) {
                        var type = typeof data === "string" ? "text" : "number";
                        collectionSchema[key] = { type: type, required: false, expandable: false };
                        requireSync = true;
                    }
                }
            }
        }

        //
        // sync the table (alter its structure) if necessary
        //
        if ( requireSync ) {
            return syncTable( {
                'tableName' : collectionID,
                'attrs' : collectionSchema
            }, false, true).then( function() {
                return q.resolve();
            }, function(e) {
                throw new Error(e);
            });
        } else {
            return q.resolve();
        }
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
        var collectionSchema = schema[collectionID];

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

            postgresObj.init(collectionID).then( function() {
                if ( typeof data !== "object" ) {
                    // the data is a primitive
                    // therefore its a table with a single column, whose value is that primitive
                } else if ( data && !data['_id'] ) {
                    // the data is an object
                    data._id = key;
                }
            }).then( function() {
                return expandIfNecessary(collectionID, schema[collectionID], key, data);
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

            postgresObj.init(collectionID)
                .then( function() {
                    return expandIfNecessary(collectionID, schema[collectionID], null, criteria);
                }).then( function() {
                    var where = [];

                    if ( criteria ) {
                        for ( var dataKey in criteria ) {

                            if ( criteria.hasOwnProperty(dataKey) ) {
                                var original = dataKey;
                                dataKey = sanitize(dataKey);

                                if ( schema[collectionID] && schema[collectionID][dataKey]) {
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
            postgresObj.init(collectionID).then( function() {
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

            postgresObj.init().then( function() {
                return q.resolve();
            }).then( function() {
                return startTx();
            }).then( function() {
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
            )
                .catch( function(e) {
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

        query: function(sql) {
            return query(sql);
        },

        init: function(collectionID) {
            collectionID = collectionID ? collectionID.toLowerCase() : undefined;
            if ( !collectionID ) {
                return q.resolve();
            }

            var p = q.defer();

            function readSchema() {
                if ( !collectionID || analyzed[collectionID] ) {
                    return q.resolve();
                }

                var deferred = q.defer();
                db.getClient().then( function(dbClient) {
                    postgresDialect.getCollectionProperties( dbClient.rawClient(), collectionID, function(err, result) {
                        if ( !err && result ) {
                            registerTable( collectionID, result );
                        }
                        analyzed[collectionID] = true;
                        dbClient.release();
                        deferred.resolve();
                    });
                });

                return deferred.promise;
            }

            function analyze() {
                readSchema().then( function( ){
                    if (toSync[collectionID]) {
                        // syncing is required, do it
                        var table = {
                            'tableName': collectionID,
                            'attrs': toSync[collectionID]
                        };
                        syncTable(table, false, false).then(function () {
                            p.resolve();
                        });
                    } else {
                        p.resolve();
                    }
                });
            }

            analyze();

            return p.promise;
        },

        sync: function( toSync, dropIfExists ) {
            toSync = toSync || {};
            var p = q.defer();

            postgresObj.init().then( function() {

                var proms = [];
                for ( var key in toSync ) {
                    if ( toSync.hasOwnProperty(key) ) {
                        var table = {
                            'tableName' : key,
                            'attrs' : toSync[key]
                        };
                        proms.push( syncTable(table, dropIfExists));
                    }
                }

                q.all(proms).then( function() {
                    p.resolve();
                });

            });

            return p.promise;
        }

    };

    return postgresObj;
};
