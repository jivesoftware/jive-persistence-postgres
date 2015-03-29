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
}

PostgresClient.prototype.query = function(sql) {
    var self = this;
    var p = q.defer();
    jive.logger.debug(sql);
    self.client.query(sql, function(err, result) {
        if(err) {
            jive.logger.error(err);
            self.errorHandler(err);
            p.reject(err);
        }
        self.doneHandler();
        p.resolve(result);
    });

    return p.promise;
};

PostgresClient.prototype.rawClient = function() {
    return this.client;
};

PostgresClient.prototype.release = function() {
    this.doneHandler(this.client);
};


module.exports = PostgresClient;
