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

q.longStackSupport = true;

/**
 * Constructor
 * @param client object
 * @param doneHandler function
 * @param errorHandler function
 * @constructor
 */
function PostgresClient(client, doneHandler, errorHandler) {
    this.client = client;
    this.doneHandler = doneHandler;
    this.errorHandler = errorHandler;
    this.result = null;
    this.released = false;
}

PostgresClient.prototype.query = function(sql, values) {
    var self = this;
    var p = q.defer();
    try {
        jive.logger.debug(sql, values);
        self.client.query(sql, values, function(err, result) {
            if (self.errorHandler(err)) {
                jive.logger.error(err);
                self.result = null;
                p.reject(err);
                return;
            }
            self.result = result;
            p.resolve(self);
        });
    } catch ( e ) {
        // no matter what ... always call done
        console.log(e.stack);
        self.doneHandler(self.client);
        self.released = true;
        p.reject(e);
    }

    return p.promise;
};

PostgresClient.prototype.rawClient = function() {
    return this.client;
};

PostgresClient.prototype.release = function() {
    if ( !this.released) {
        this.doneHandler();
        this.released = true;
    }
};

PostgresClient.prototype.results = function() {
    return this.result;
};

module.exports = PostgresClient;
