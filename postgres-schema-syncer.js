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
var flat = require('flat');
var postgresDialect = require('sql-ddl-sync/lib/Dialects/postgresql');

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Public API

/**
 * Constructor
 * @param db
 * @param schema optional
 * @constructor
 */
function PostgresSchemaSyncer( db, schema ) {
    this.db = db;
    this.schema = {};
    this.toSync = {};
    this.analyzed = {};
    if ( schema ) {
        this.toSync = schema;
        if ( this.toSync ) {
            for ( var k in this.toSync ) {
                if (this.toSync.hasOwnProperty(k) ) {
                    var value = this.toSync[k];
                    delete this.toSync[k];
                    this.toSync[k.toLowerCase()] = value;
                }
            }
        }
    }
}

module.exports = PostgresSchemaSyncer;

PostgresSchemaSyncer.prototype.syncTable = syncTable;
PostgresSchemaSyncer.prototype.prepCollection = prepCollection;
PostgresSchemaSyncer.prototype.syncCollections = syncCollections;
PostgresSchemaSyncer.prototype.expandIfNecessary = expandIfNecessary;
PostgresSchemaSyncer.prototype.getTableSchema = getTableSchema;

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Private

function throwError(detail) {
    var error = new Error(detail);
    jive.logger.error(error.stack);
    throw error;
}

function sanitize(key) {
    return key.replace('.', '_');
}

function getTableSchema(table) {
    if (!this.schema) {
        return null;
    }
    return this.schema[table];
}

function query(sql) {
    return this.db.query(sql).then( function(dbClient) {
        dbClient.release();
        return dbClient;
    });
}

function tableExists(table) {
    var self = this;
    return query.call(self, "select * from pg_tables where tablename='" + table + "'").then( function(client) {
        var r = client.results();
        return r && r.rowCount > 0;
    }, function(e) {
        return q.reject(e);
    });
}

function dropTable(table) {
    var self = this;
    return query.call(self, "drop table if exists \"" + table + "\"").then( function(r) {
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
            this.schema[collectionID] = tableAttrs;
        }
    }
}

function syncTable( table, dropIfExists, force ) {
    var p = q.defer();
    var self = this;

    var collectionID = table['tableName'];
    collectionID = collectionID.replace('"','');
    collectionID = collectionID.toLowerCase();

    var tableAttrs = table['attrs'];
    if ( !tableAttrs['_id'] ) {
        tableAttrs['_id'] = { type: "text", required: true, index: true, unqiue: true };
    }

    registerTable.call( self, collectionID, tableAttrs);

    q.resolve().then( function() {
        // start a transaction
        return q.resolve();
    }).then( function() {
        // check if table exists
        return tableExists.call(self, collectionID);
    }).then( function(exists) {
        if ( (exists && !force) && !dropIfExists ) {
            // nothing to do:
            // - the table exists, and we're not forcing any changes
            // - we are not dropping the table
            delete self.toSync[collectionID];
            return q.resolve({
                exists : exists
            });
        } else {
            // a sync operation is required; grab a client
            return self.db.getClient().then( function(client) {
                return {
                    client : client,
                    exists : exists
                }
            }).fail( function(e) {
                return q.reject(e);
            });
        }
    }).then( function(r) {
        var dbClient = r.client;
        var exists = r.exists;
        var syncDeferred = q.defer();

        if ( dbClient && (!exists || force) ) {
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
                    throwError(err);
                } else {
                    jive.logger.info("> Sync Done", collectionID );
                    dbClient.release();
                    delete self.toSync[collectionID];
                    syncDeferred.resolve();
                }
            });

        } else if (dropIfExists ) {
            return dropTable.call(self, collectionID).then( function() {
                return syncTable.call(self, table, false).then( function() {
                    if ( dbClient ) {
                        dbClient.release();
                    }
                    syncDeferred.resolve();
                }, function(e) {
                    if ( dbClient ) {
                        dbClient.release();
                    }
                    throwError(e);
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
    }).then(

        // success
        function() {
            p.resolve();
        },

        // error
        function(e) {
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
    var self = this;
    var requireSync;
    var lazyCreateCollection = true; // todo -- parameterize

    if ( !collectionSchema ) {
        // collection doesn't exist
        if ( lazyCreateCollection ) {
            collectionSchema = {};
            this.schema[collectionID] = collectionSchema;
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
        return syncTable.call(self,  {
            'tableName' : collectionID,
            'attrs' : collectionSchema
        }, false, true).then( function() {
            return q.resolve();
        }, function(e) {
            throwError(e);
        });
    } else {
        return q.resolve();
    }
}

function prepSchema() {
    var self = this;
    if (!self.toSync || Object.keys(self.toSync).length < 1 ) {
        return q.resolve();
    } else {
        var promises = [];
        for ( var k in self.toSync ) {
            if (self.toSync.hasOwnProperty(k) ) {
                var value = self.toSync[k];
                var table = {
                    'tableName' : k,
                    'attrs' : value
                };
                promises.push( syncTable.bind(self, table) );
            }
        }

        return qSerial(promises);
    }
}

function prepCollection(collectionID) {
    var self = this;
    return prepSchema.call(this).then( function() {

        collectionID = collectionID ? collectionID.toLowerCase() : undefined;
        if ( !collectionID ) {
            return q.resolve();
        }

        var p = q.defer();

        function readSchema() {
            if ( !collectionID || self.analyzed[collectionID] ) {
                delete self.toSync[collectionID];
                return q.resolve();
            }

            var deferred = q.defer();
            self.db.getClient().then( function(dbClient) {
                postgresDialect.getCollectionProperties( dbClient.rawClient(), collectionID, function(err, result) {
                    if ( !err && result ) {
                        registerTable.call( self, collectionID, result );
                    }
                    self.analyzed[collectionID] = true;
                    dbClient.release();
                    deferred.resolve();
                });
            }).fail( function(e) {
                deferred.reject(e);
            });

            return deferred.promise;
        }

        function analyze() {
            readSchema().then( function( ){
                if (self.toSync[collectionID]) {
                    // syncing is required, do it
                    var table = {
                        'tableName': collectionID,
                        'attrs': self.toSync[collectionID]
                    };
                    syncTable.call(self, table, false, false).then(function () {
                        p.resolve();
                    });
                } else {
                    p.resolve();
                }
            }).fail( function(e) {
                p.reject(e);
            });
        }

        analyze();

        return p.promise;
    });
}

function syncCollections( collectionsToSync, dropIfExists ) {
    collectionsToSync = collectionsToSync || {};
    var p = q.defer();

    var proms = [];
    for ( var key in collectionsToSync ) {
        if ( collectionsToSync.hasOwnProperty(key) ) {
            var table = {
                'tableName' : key,
                'attrs' : collectionsToSync[key]
            };
            proms.push( syncTable.call( this, table, dropIfExists ) );
        }
    }

    q.all(proms).then( function() {
        p.resolve();
    });

    return p.promise;
}

/**
 * Runs promise producing functions in serial.
 * @param funcs
 * @returns {*}
 */
function qSerial(funcs) {
    return qParallel(funcs, 1);
}

/**
 * Runs at most 'count' number of promise producing functions in parallel.
 * @param funcs
 * @param count
 * @returns {*}
 */
function qParallel(funcs, count) {
    var length = funcs.length;
    if (!length) {
        return q([]);
    }

    if (count == null) {
        count = Infinity;
    }

    count = Math.max(count, 1);
    count = Math.min(count, funcs.length);

    var promises = [];
    var values = [];
    for (var i = 0; i < count; ++i) {
        var promise = funcs[i]();
        promise = promise.then(next(i));
        promises.push(promise);
    }

    return q.all(promises).then(function () {
        return values;
    });

    function next(i) {
        return function (value) {
            if (i == null) {
                i = count++;
            }

            if (i < length) {
                values[i] = value;
            }

            if (count < length) {
                return funcs[count]().then(next())
            }
        }
    }
}
