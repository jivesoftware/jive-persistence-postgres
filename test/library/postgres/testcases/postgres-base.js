var assert = require('assert');
var q = require('q');
var test = require('../basePersistenceTest');

describe('jive', function () {

    describe ('#persistence.postgres-base', function () {

        it('basic crud', function (done) {
            var jive = this['jive'];
            var persistenceBase = this['persistenceBase'];

            // drop table
            persistenceBase.query('drop table if exists test_table')
            .then( function(r) {
                // prepare table table
                return persistenceBase.query('create table test_table (test_column1 int)');
            })
            .then( function(r) {
                // create
                return persistenceBase.query('insert into test_table values (1000)');
            })
            .then( function(r) {
                // read
                return persistenceBase.query('select * from test_table').then( function(results) {
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
                return persistenceBase.query('update test_table set test_column1 = 2000')
                    .then( function(results) {
                        if ( !results ) {
                            assert.fail('Empty results', 'non empty result');
                        }
                        return persistenceBase.query('select * from test_table');
                    })
                    .then( function(results) {
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
                return persistenceBase.query('delete from test_table')
                    .then( function(results) {
                        if ( !results ) {
                            assert.fail('Empty results', 'non empty result');
                        }
                        return persistenceBase.query('select * from test_table');
                    })
                    .then( function(results) {
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
            persistenceBase.query('drop table if exists test_table')
            .then( function(r) {
                // prepare table table
                return persistenceBase.query('create table test_table (test_column1 int)');
            })
            .then( function(r) {
                // commit
                return persistenceBase.startTx()
                    .then( function() {
                        return persistenceBase.query('insert into test_table values (1000)');
                    })
                    .then( function() {
                        return persistenceBase.commitTx()
                    })
                    .then( function(r) {
                        return persistenceBase.query('select * from test_table').then( function(results) {
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
                return persistenceBase.query('drop table if exists test_table')
                    .then( function(r) {
                        // prepare table table
                        return persistenceBase.query('create table test_table (test_column1 int)');
                    })
                    .then( function() {
                        return persistenceBase.startTx()
                    })
                    .then( function() {
                        return persistenceBase.query('insert into test_table values (1000)');
                    })
                    .then( function() {
                        return persistenceBase.rollbackTx()
                    })
                    .then( function(r) {
                        return persistenceBase.query('select * from test_table').then( function(results) {
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
            .catch( function(e) {
                assert.fail(e);
            }).finally( function() {
                done();
            });

        });

    });
});

