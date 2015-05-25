var amqp = require('./lib/amqp.js');
var settings = require('./lib/settings.js').readFrom(process.env);
var Q = require('q');
var util = require('util');
var _ = require('lodash');

var execId = "test_exec2";

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

function QueueCreator(channel){

    var REBOUND_QUEUE_TTL = 10 * 60 * 1000; // 10 min

    function getOrderedSteps(task){
        var firstNode = _.findWhere(task.recipe.nodes, {"first" : true});
        var nodeId = firstNode.id;
        var result = [nodeId];
        while (true) {
            var connection = _.findWhere(task.recipe.connections, {"from" : nodeId});
            if (!connection) break;
            nodeId = connection["to"];
            result.push(nodeId);
        }
        return result;
    }

    function printSailorSettings(sailorSettings, stepNum, stepId){
        console.log('--------------------------------------------');
        console.log('Step %s (%s) sailor .env vars:', stepNum, stepId);
        _.forOwn(sailorSettings, function(value, key){
            console.log('%s=%s', key, value);
        });
        console.log('--------------------------------------------');
    }

    function makeQueuesForTheTask(task, execId){

        var steps = getOrderedSteps(task);

        var exchangeName = getExchangeName(task.user);

        return assertExchange(exchangeName).then(function promiseCreateQueues(){

            var promises = [];
            var stepsSailorSettings = {};

            // loop through steps
            for (var i = 0; i < steps.length; i++) {

                var stepId = steps[i];
                var stepPromises = [];

                var messagesQueue = getQueueName(task.id, stepId, execId, "messages"); // step1:messages (incoming messages)
                var errorsQueue = getQueueName(task.id, stepId, execId, "errors"); // step1:errors (errors)
                var reboundsQueue = getQueueName(task.id, stepId, execId, "rebounds"); // step1:rebounds (rebounds)

                var reboundReturnTag   = getRoutingTag(task.id, stepId, execId, "reboundreturn"); // step1.reboundreturn (incoming msg)
                var resultTag = getRoutingTag(task.id, stepId, execId, "result"); // step1.result (outgoing msg)
                var errorTag   = getRoutingTag(task.id, stepId, execId, "error"); // step1.error (error)
                var reboundTag = getRoutingTag(task.id, stepId, execId, "rebound"); // step1.rebound (rebound)

                stepPromises.push(assertQueue(messagesQueue));
                stepPromises.push(assertQueue(errorsQueue));
                stepPromises.push(assertReboundsQueue(reboundsQueue, exchangeName, reboundReturnTag)); // return rebounds to messages

                stepPromises.push(subscribeQueueToKey(messagesQueue, exchangeName, reboundReturnTag)); // listen messages
                stepPromises.push(subscribeQueueToKey(errorsQueue, exchangeName, errorTag)); // listen errors
                stepPromises.push(subscribeQueueToKey(reboundsQueue, exchangeName, reboundTag)); // listen rebounds

                if (steps[i-1]) {
                    var prevStepResultsTag = getRoutingTag(task.id, steps[i-1], execId, "result");
                    stepPromises.push(subscribeQueueToKey(messagesQueue, exchangeName, prevStepResultsTag)); // listen results from prev. step
                }

                var sailorSettings = {
                    "TASK":JSON.stringify(task),
                    "STEP_ID": stepId,
                    "LISTEN_MESSAGES_ON" : messagesQueue,
                    "PUBLISH_MESSAGES_TO" : exchangeName,
                    "ERROR_ROUTING_KEY" : errorTag,
                    "REBOUND_ROUTING_KEY" : reboundTag,
                    "DATA_ROUTING_KEY" : resultTag
                };

                var stepPromise = Q.all(stepPromises).then(printSailorSettings.bind(null, sailorSettings, i+1, stepId));
                promises.push(stepPromise);

                stepsSailorSettings[i] = sailorSettings;
            }

            // when all necessary queues are created
            return Q.all(promises).then(function(){
                console.log('All queues are created');
            });
        });
    }

    function getExchangeName(userId){
        return "exchange:" + userId;
    }

    function getQueueName(taskId, stepId, execId, type) {
        return util.format("%s:%s:%s:%s", taskId, stepId, execId, type);
    }

    function getRoutingTag(taskId, stepId, execId, type) {
        return util.format("%s.%s.%s.%s", taskId, stepId, execId, type);
    }

    function assertExchange(exchangeName) {
        var type = 'direct';
        var options = {
            durable: true,
            autoDelete: false
        };
        return channel.assertExchange(exchangeName, type, options).then(function assertExchangeSuccess() {
            console.log('Created exchange %s', exchangeName);
        });
    }

    function assertQueue(queueName) {
        var options = {
            durable: true,
            autoDelete: false
        };
        return channel.assertQueue(queueName, options).then(function assertQueueSuccess() {
            console.log('Created queue %s', queueName);
        });
    }

    function assertReboundsQueue(queueName, returnToExchange, returnWithKey) {
        var options = {
            durable: true,
            autoDelete: false,
            arguments: {
                'x-message-ttl': REBOUND_QUEUE_TTL,
                'x-dead-letter-exchange': returnToExchange, // send dead rebounded queues back to exchange
                'x-dead-letter-routing-key': returnWithKey // with tag as message
            }
        };

        return channel.assertQueue(queueName, options).then(function assertQueueSuccess() {
            console.log('Created queue %s', queueName);
        });
    }

    function subscribeQueueToKey(queueName, exchangeName, routingKey){
        return amqpConnection.publishChannel.bindQueue(queueName, exchangeName, routingKey).then(function assertQueueSuccess() {
            console.log('Send keys %s to queue %s ', routingKey,  queueName);
        });
    }

    this.makeQueuesForTheTask = makeQueuesForTheTask;
}


var amqpConnection = new amqp.AMQPConnection(settings);

amqpConnection.connect(settings.AMQP_URI).then(function() {

    var channel = amqpConnection.publishChannel;

    var queueCreator = new QueueCreator(channel);
    queueCreator.makeQueuesForTheTask(task, execId).then(function(){
        process.exit(0);
    })
});




