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

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Public API

function PostgresSqlAdaptor(schemaProvider) {
    this.schemaProvider = schemaProvider;
}

module.exports = PostgresSqlAdaptor;

PostgresSqlAdaptor.prototype.createUpdateSQL = createUpdateSQL;
PostgresSqlAdaptor.prototype.createInsertSQL = createInsertSQL;
PostgresSqlAdaptor.prototype.createSelectSQL = createSelectSQL;
PostgresSqlAdaptor.prototype.createDeleteSQL = createDeleteSQL;
PostgresSqlAdaptor.prototype.hydrateResults = hydrateResults;

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Private
function isValue(value) {
    return value || typeof value === 'number';
}

function throwError(detail) {
    var error = new Error(detail);
    jive.logger.error(error.stack);
    throw error;
}

function sanitize(key) {
    return key.replace('.', '_');
}

function hydrate(row) {
    var toUnflatten = {};
    var needFlatten;

    /*** 1 of 3 - VARIABLE FOR STORING OAUTH KEY IF FOUND ***/
    var oauthKey = null;

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
                /*** 2 of 3 - IF KEY IS "oauth" THEN WE HOLD ONTO THE RAW VALUE ***/
                if (dataKey === "oauth") {
                  oauthKey = value;
                } // end if
            }
        }
    }


    var obj = toUnflatten;
    if (needFlatten) {
      obj = flat.unflatten(toUnflatten, {'delimiter': '_'});
      /*** 3 of 3 - ADDED OPTION FOR OAUTH KEYS STORED IN JSON STRUCTURES, WHICH FLATTEN CORRUPTS TO "access" : {token : "xxxxx" } rather than preserve access_token; ***/
      if (oauthKey) {
        obj["oauth"] = oauthKey;
      } // end if
    } // end if
    delete obj[""];
    return obj;
}

function buildQueryArguments(collectionID, data, key) {
    var self = this;
    var keys = [], values = [], sanitized = {}, dataToSave = {};
    var collectionSchema = self.schemaProvider.getTableSchema(collectionID);

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

function createUpdateSQL(collectionID, data, key) {
    var self = this;
    // try to update first
    var structure = buildQueryArguments.call(self, collectionID, data, key);
    var values = structure.values;
    var keys = structure.keys;
    if (values.length < 1) {
        throwError("cannot insert empty data");
    }

    var sql = "update \"" + collectionID + "\" set";
    for ( var i = 0 ; i < keys.length; i++ ) {
        sql += " " + keys[i] + "= " + values[i]
            + ( ( i < keys.length - 1 ) ? "," : "");
    }
    sql += " where _id='" + key + "'";

    return sql;
}

function createInsertSQL(collectionID, data, key) {
    var self = this;
    var structure = buildQueryArguments.call(self, collectionID, data, key);
    var values = structure.values;
    var keys = structure.keys;
    if (values.length < 1) {
        var error = new Error("cannot insert empty data");
        jive.logger.error(error.stack);
    }

    var sql = "insert into \"" + collectionID + "\" ( " + keys.join(',') + " ) " +
        "values ( " + values.join(',') + ")";

    return sql;
}

function createSelectSQL(collectionID, criteria, limit) {
    var where = [];
    var self = this;

    if ( criteria ) {
        for ( var dataKey in criteria ) {

            if ( criteria.hasOwnProperty(dataKey) ) {
                var original = dataKey;
                dataKey = sanitize(dataKey);

                var tableSchema = self.schemaProvider.getTableSchema(collectionID);
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
                    throwError(collectionID + "." + dataKey + " does not exist");
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

    return sql;
}

function createDeleteSQL(collectionID, key ) {
    if ( key ) {
        return "delete from \"" + collectionID + "\" where _id = '" + key + "'";
    } else {
        return "delete from \"" + collectionID + "\"";
    }
}

function hydrateResults(r) {
    var results = [];

    // build a json structure from the results, based on '_' delimiter
    if (r.rows['indexOf']) {
        r.rows.forEach( function(row) {
            var obj = hydrate(row);
            results.push(obj);
        });
    } else {
        results.push(hydrate(r.rows));
    }

    return results;
}
