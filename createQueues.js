var amqp = require('./lib/amqp.js');
var settings = require('./lib/settings.js').readFrom(process.env);
var _ = require('lodash');
var QueueCreator = require('./queueCreator.js').QueueCreator;

var execId = "test_exec3";

var task = {
    "id" : "5559edd38968ec0736000003",
    "user" : "5527f0ea43238e5d5f000001",
    "data" : {
        "step_2" : {
            "_account" : "554b53aed5178d6540000001"
        },
        "step_3" : {
            "mapper" : {
                "data" : {
                    "qty" : "2"
                },
                "product" : "Btestsku"
            }
        },
        "step_1" : {
            "interval" : "minute",
            "_account" : "5559ed6b8968ec0736000002"
        }
    },
    "recipe" : {
        "nodes" : [
            {
                "first" : true,
                "id" : "step_1",
                "function" : "getProducts",
                "compId" : "shopware"
            },
            {
                "id" : "step_3",
                "function" : "map",
                "compId" : "mapper"
            },
            {
                "id" : "step_2",
                "function" : "updateInventory",
                "compId" : "magento"
            }
        ],
        "connections" : [
            {
                "to" : "step_3",
                "from" : "step_1"
            },
            {
                "to" : "step_2",
                "from" : "step_3"
            }
        ]
    }
};



var amqpConnection = new amqp.AMQPConnection(settings);

amqpConnection.connect(settings.AMQP_URI).then(function() {

    var channel = amqpConnection.publishChannel;

    var queueCreator = new QueueCreator(channel);

    /**
     * queueCreator.makeQueuesForTheTask(task, execId) returns JSON object which contains .env vars for sailor for each of steps
     * {"0": {...}, "1": {...}, "2": {....}} where 0,1,2 is step number in the flow
     * and where each {...} contains TASK, STEP_ID, LISTEN_MESSAGES_ON, PUBLISH_MESSAGES_TO, ERROR_ROUTING_KEY, REBOUND_ROUTING_KEY, DATA_ROUTING_KEY
     */

    queueCreator.makeQueuesForTheTask(task, execId).then(function(stepEnvVars){
        _.forEach(stepEnvVars, function (envVars, stepNumber) {
            console.log('------------Step %s sailor settings-------------', stepNumber);
            _.forOwn(envVars, function(value, key){
                console.log('%s=%s', key, value);
            });
        });
        console.log('------------end-------------');
    }).fail(function (err) {
        console.log(err);
    }).done();
});


