var assert = require('assert');
var q = require('q');
    q.longStackSupport = true;
var test = require('../basePersistenceTest');

describe('jive', function () {

    describe ('#persistence.postgres-base', function () {

        it('basic crud', function (done) {
            var jive = this['jive'];
            var persistenceBase = this['persistenceBase'];

            persistenceBase.getClient().then( function(dbClient) {

                // drop table
                dbClient
                    .query('drop table if exists test_table1')
                    .then( function(r) {
                        // prepare table table
                        return dbClient.query('create table test_table1 (test_column1 int)');
                    })
                    .then( function(r) {
                        // create
                        return dbClient.query('insert into test_table1 values (1000)');
                    })
                    .then( function(r) {
                        // read
                        return dbClient.query('select * from test_table1').then( function() {
                            var results = dbClient.results();
                            if ( !results ) {
                                assert.fail('Empty results', 'non empty result');
                            }
                            var rows = results.rows;
                            if ( !rows || rows.length < 1 ) {
                                assert.fail('Zero length rows', 'one row');
                            }

                            var returned = rows[0];
                            if ( returned['test_column1'] !== 1000) {
                                assert.fail(returned['test_column1'], 1000);
                            }

                            return q.resolve();
                        });
                    })
                    .then( function(r) {
                        // update
                        return dbClient.query('update test_table1 set test_column1 = $1', [2000])
                        .then( function() {
                            var results = dbClient.results();
                            if ( !results ) {
                                assert.fail('Empty results', 'non empty result');
                            }
                            return dbClient.query('select * from test_table1');
                        })
                        .then( function() {
                            var results = dbClient.results();

                            if ( !results ) {
                                assert.fail('Empty results', 'non empty result');
                            }
                            var rows = results.rows;
                            if ( !rows || rows.length < 1 ) {
                                assert.fail('Zero length rows', 'one row');
                            }

                            var returned = rows[0];
                            if ( returned['test_column1'] !== 2000) {
                                assert.fail(returned['test_column1'], 2000);
                            }

                            return q.resolve();
                        });
                    })
                    .then( function(r) {
                        // delete
                        return dbClient.query('delete from test_table1')
                        .then( function() {
                            var results = dbClient.results();
                            if ( !results ) {
                                assert.fail('Empty results', 'non empty result');
                            }
                            return dbClient.query('select * from test_table1');
                        })
                        .then( function() {
                            var results = dbClient.results();
                            if ( !results ) {
                                assert.fail('Empty results', 'non empty result');
                            }
                            var rows = results.rows;
                            if ( rows.length > 0 ) {
                                assert.fail('Non-zero rows', 'zero rows');
                            }

                            return q.resolve();
                        });
                    })
            })
            .catch( function(e) {
                assert.fail(e);
            }).finally( function() {
                done();
            });
        });

        it('transactions', function (done) {
            var jive = this['jive'];
            var persistenceBase = this['persistenceBase'];

            // drop table
            persistenceBase.getClient().then( function(dbClient) {
                return dbClient.query('drop table if exists test_table2')
                    .then( function(r) {
                        // prepare table table
                        return dbClient.query('create table test_table2 (test_column1 int)');
                    })
                    .then( function(r) {
                        // commit
                        return dbClient.query("begin")
                            .then( function() {
                                return dbClient.query('insert into test_table2 values (1000)');
                            })
                            .then( function() {
                                return dbClient.query("commit")
                            })
                            .then( function(r) {
                                return dbClient.query('select * from test_table2').then( function() {
                                    var results = dbClient.results();
                                    if ( !results || !results.rows || results.rows.length < 1 ) {
                                        assert.fail('Empty results', 'non empty result');
                                    }
                                    var returned = results.rows[0];
                                    if ( returned['test_column1'] !== 1000) {
                                        assert.fail(returned['test_column1'], 1000, 'failed to commit insert');
                                    }

                                    return q.resolve();
                                });
                            })
                    })
                    .then( function(r) {
                        // rollback
                        return dbClient.query('drop table if exists test_table2')
                            .then( function(r) {
                                // prepare table table
                                return dbClient.query('create table test_table2 (test_column1 int)');
                            })
                            .then( function() {
                                return dbClient.query("begin")
                            })
                            .then( function() {
                                return dbClient.query('insert into test_table2 values (1000)');
                            })
                            .then( function() {
                                return dbClient.query('insert into test_table2 values (2000)');
                            })
                            .then( function() {
                                return dbClient.query('insert into test_table2 values (3000)');
                            })
                            .then( function() {
                                return dbClient.query("rollback")
                            })
                            .then( function(r) {
                                return dbClient.query('select * from test_table2').then( function() {
                                    var results = dbClient.results();
                                    if ( !results || !results.rows ) {
                                        assert.fail('Empty results', 'non empty result');
                                    }
                                    if ( results.rows.length > 0 ) {
                                        assert.fail('found rows', 'found no rows', 'failed to rollback insert');
                                    }

                                    return q.resolve();
                                });
                            })
                    })

            })
            .catch( function(e) {
                assert.fail(e);
            }).finally( function() {
                done();
            });

        });

    });
});

