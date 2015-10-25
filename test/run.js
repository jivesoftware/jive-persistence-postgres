var testUtils = require('jive-testing-framework/testUtils');
var jive = require('jive-sdk');
var jivePostgres = require('../postgres-dynamic');
var jivePostgresBase = require('../postgres-base');

var makeRunner = function() {
    return testUtils.makeRunner( {
        'eventHandlers' : {
            'onTestStart' : function(test) {
                test['ctx']['persistence'] = new jivePostgres({
                    'databaseUrl' : 'pg://postgres:postgres@localhost:5432/mydb',
                    'dbPoolSize' : '15'
                });
                test['ctx']['persistenceBase'] = new jivePostgresBase({
                    'databaseUrl' : 'pg://postgres:postgres@localhost:5432/mydb',
                    'dbPoolSize' : '15'
                });
            },
            'onTestEnd' : function(test) {
                test['ctx']['persistence'].destroy();
                test['ctx']['persistenceBase'].destroy();
            }
        }
    });
};

makeRunner().runTests(
    {
        'context' : {
            'testUtils' : testUtils,
            'jive' : jive,
            'jivePostgres' : jivePostgres
        },
        'rootSuiteName' : 'jive',
        'runMode' : 'test',
        'testcases' : process.cwd()  + '/library',
        'timeout' : 500000
    }
).then( function(allClear) {
    if ( allClear ) {
        process.exit(0);
    } else {
        process.exit(-1);
    }
});