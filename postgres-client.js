var q = require('q');
var jive = require('jive-sdk');

function PostgresClient(client, doneHandler, errorHandler) {
    this.client = client;
    this.doneHandler = doneHandler;
    this.errorHandler = errorHandler;
}

PostgresClient.prototype.query = function(sql) {
    var self = this;
    var p = q.defer();
    jive.logger.info(sql);
    self.client.query(sql, function(err, result) {
        if(err) {
            self.errorHandler(err);
            p.reject(err);
        }
        self.doneHandler();
        p.resolve(result);
    });

    return p.promise;
};

module.exports = PostgresClient;
