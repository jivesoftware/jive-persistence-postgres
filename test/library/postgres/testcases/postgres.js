var assert = require('assert');
var test = require('../basePersistenceTest');

describe('jive', function () {

    describe ('#persistence.postgres', function () {

        it('test', function (done) {
            var jive = this['jive'];
            var testUtils = this['testUtils'];
            var persistence = this['persistence'];

            var toSync = [
                {
                    tableName: 'tbl',
                    attrs: {
                        data: { type: "text", required: true }
                    }
                },

                {
                    tableName: 'myCollection',
                    attrs: {
                        key:       { type: "text", required: false },
                        data_name: { type: "text", required: false },
                        data_age:  { type: "text", required: false }
                    }
                },

                {
                    tableName: 'myOtherCollection',
                    attrs: {
                        data_number:    { type: "text", required: false }
                    }
                }
            ];

            persistence.sync( toSync, true)
            .then( function() {
                return test.testSave(testUtils, persistence, 'tbl');
            })
            .then( function() {
                return test.testFind(testUtils, persistence);
            })
            .then( function() {
                return test.testRemove(testUtils, persistence, 'tbl');
            })
            .catch( function(e) {
                assert.fail(e);
            })
            .finally( function() {
                done();
            });

        });

    });
});

