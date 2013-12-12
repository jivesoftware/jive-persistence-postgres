var testUtils = require('jive-testing-framework/testUtils');
var jive = require('jive-sdk');
var jivePostgres = require('../');

var makeRunner = function() {
    return testUtils.makeRunner( {
        'eventHandlers' : {
            'onTestStart' : function(test) {
                test['ctx']['persistence'] = new jivePostgres({
                    'databaseUrl' : 'pg://postgres:postgres@localhost:5432/mydb'
                });
            },
            'onTestEnd' : function(test) {
                test['ctx']['persistence'].destroy();
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
        'timeout' : 5000
    }
).then( function(allClear) {
    if ( allClear ) {
        process.exit(0);
    } else {
        process.exit(-1);
    }
});