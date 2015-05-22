var amqp = require('./lib/amqp.js');
var settings = require('./lib/settings.js').readFrom(process.env);
var Q = require('q');
var util = require('util');
var _ = require('lodash');

var execId = "1432205514864";

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

var REBOUND_QUEUE_TTL = 10 * 60 * 1000; // 10 min

function getExchangeName(userId){
    return "exchange:" + userId;
}

function getQueueName(execId, taskId, stepId, type) {
    return util.format("%s:%s:%s:%s", execId, taskId, stepId, type);
}

function getRoutingTag(execId, taskId, stepId, type) {
    return util.format("%s.%s.%s.%s", execId, taskId, stepId, type);
}

function assertExchange(exchangeName) {
    var type = 'direct';
    var options = {
        durable: true,
        autoDelete: false
    };
    return channel.assertExchange(exchangeName, type, options).then(function assertExchangeSuccess() {
        console.log('Succesfully asserted exchange: ' + exchange.name);
    });
}

function assertQueue(queueName) {
    var options = {
        durable: true,
        autoDelete: false
    };
    return channel.assertQueue(queueName, options).then(function assertQueueSuccess() {
        console.log('Succesfully asserted queue: ' + queueName);
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
        console.log('Succesfully asserted queue: ' + queueName);
    });
}

function subscribeQueueToKey(queueName, exchangeName, routingKey){
    return amqpConnection.publishChannel.bindQueue(queueName, exchangeName, routingKey).then(function assertQueueSuccess() {
        console.log('Succesfully subscribed queue ' + queueName + ' to ' + routingKey);
    });
}

var amqpConnection = new amqp.AMQPConnection(settings);

amqpConnection.connect(settings.AMQP_URI).then(function() {

    var channel = amqpConnection.publishChannel;

    makeQueuesForTheTask(task, execId);

});


function getTaskSteps(task){
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


function makeQueuesForTheTask(task, execId){

    var steps = getTaskSteps(task);

    for (var i = 0; i < steps.length; i++) {

        var stepId = steps[i];

        var messagesQueue = getQueueName(execId, task.id, stepId, "messages"); // step1:messages (incoming messages)
        var errorsQueue = getQueueName(execId, task.id, stepId, "errors"); // step1:errors (errors)
        var reboundsQueue = getQueueName(execId, task.id, stepId, "rebounds"); // step1:rebounds (rebounds)

        var messageTag   = getRoutingTag(execId, task.id, stepId, "message"); // step1.message (incoming msg)
        var resultTag = getRoutingTag(execId, task.id, stepId, "result"); // step1.result (outgoing msg)
        var errorTag   = getRoutingTag(execId, task.id, stepId, "error"); // step1.error (error)
        var reboundTag = getRoutingTag(execId, task.id, stepId, "rebound"); // step1.rebound (rebound)

        assertQueue(messagesQueue);
        assertQueue(errorsQueue);
        assertReboundsQueue(reboundsQueue, messageTag); // return rebounds to messages

        subscribeQueueToKey(messagesQueue, messageTag); // listen messages
        subscribeQueueToKey(errorsQueue, errorTag); // listen errors
        subscribeQueueToKey(reboundsQueue, reboundTag); // listen rebounds

        if (steps[i-1]) {
            var prevStepResultsTag = getRoutingTag(execId, task.id, steps[i-1], "result");
            subscribeQueueToKey(messagesQueue, prevStepResultsTag); // listen results from prev. step
        }

        var sailorSettings = {
            "TASK":JSON.stringify(task),
            "STEP_ID": stepId,
            "LISTEN_MESSAGES_ON" : messagesQueue,
            "PUBLISH_MESSAGES_TO" : exchangeName,
            "DATA_ROUTING_KEY" : resultTag,
            "ERROR_ROUTING_KEY" : errorTag,
            "REBOUND_ROUTING_KEY" : reboundTag
        };

        console.log('Step %s sailor .env vars:');
        console.log('%j', sailorSettings);

    }
}



}



console.log('%j', getOrderedSteps());

process.exit(0);



amqpConnection.connect(settings.AMQP_URI).then(function(){

    var channel = amqpConnection.publishChannel;
    var steps = getOrderedSteps();

    for (var i = 0; i < steps.length; i++) {

        var stepId = steps[i];



        var outMessageTag       = getRoutingTag(execId, task.id, stepId, "out_message"); // step1.out_message (outgoing)
        var errorTag            = getRoutingTag(execId, task.id, stepId, "error"); // step1.error (outgoing)
        var reboundTag          = getRoutingTag(execId, task.id, stepId, "rebound"); // step1.rebound (to go to rebounds queue)
        var reboundedMessageTag = getRoutingTag(execId, task.id, stepId, "rebounded_message"); // step1.rebounded_message (to go back to messages)



        assertQueue(messagesQueue);
        assertQueue(errorsQueue);
        assertQueue(reboundsQueue, reboundedMessageTag);

        subscribeQueueToKey(messagesQueue, reboundedMessageTag);
        subscribeQueueToKey(errorsQueue, errorTag);

        if (steps[i+1]) {
            var nextStepMessagesQueue = getQueueName(execId, task.id, steps[i+1], "in_messages");
            assertQueue(nextStepMessagesQueue);
            subscribeQueueToKey(nextStepMessagesQueue, outMessageTag);
        }



    var messagesQueue1 = getQueueName(execId, task.id, "step_1", "messages"); // queue for messages for step1
    var messagesQueue2 = getQueueName(execId, task.id, "step_2", "messages"); // queue for messages for step2
    var messagesQueue3 = getQueueName(execId, task.id, "step_3", "messages"); // queue for messages for step3

    var reboundsQueue1 = getQueueName(execId, task.id, "step_1", "rebounds"); // queue for rebounds of step1
    var reboundsQueue2 = getQueueName(execId, task.id, "step_2", "rebounds"); // queue for rebounds of step1
    var reboundsQueue3 = getQueueName(execId, task.id, "step_3", "rebounds"); // queue for rebounds of step1

    var messageTag1 = getRoutingTag(execId, task.id, "step_1", "message"); // tag for messages for step1
    var messageTag2 = getRoutingTag(execId, task.id, "step_2", "message"); // tag for messages for step2 (emitted by step1)
    var messageTag3 = getRoutingTag(execId, task.id, "step_3", "message"); // tag for messages for step3 (emitted by step2)
    var messageTag4 = getRoutingTag(execId, task.id, "step_4", "message"); // tag for messages emitted by step3

    var reboundTag1 = getRoutingTag(execId, task.id, "step_1", "rebound"); // tag for rebounds emitted by step1
    var reboundTag2 = getRoutingTag(execId, task.id, "step_2", "rebound"); // tag for rebounds emitted by step2
    var reboundTag3 = getRoutingTag(execId, task.id, "step_3", "rebound"); // tag for rebounds emitted by step3

    var errorTag1 = getRoutingTag(execId, task.id, "step_1", "error"); // tag for errors emitted by step1
    var errorTag2 = getRoutingTag(execId, task.id, "step_2", "error"); // tag for errors emitted by step2
    var errorTag3 = getRoutingTag(execId, task.id, "step_3", "error"); // tag for errors emitted by step3

    var sailor1 = {
        LISTEN_MESSAGES_ON : messagesQueue1,
        PUBLISH_MESSAGES_TO : exchangeName,
        DATA_ROUTING_KEY : messageTag2, // sailor1 sends messages to queue2
        ERROR_ROUTING_KEY : errorTag1,
        REBOUND_ROUTING_KEY : reboundTag1
    };

    var sailor2 = {
        LISTEN_MESSAGES_ON : messagesQueue2,
        PUBLISH_MESSAGES_TO : exchangeName,
        DATA_ROUTING_KEY : messageTag3,// sailor2 sends messages to queue3
        ERROR_ROUTING_KEY : errorTag2,
        REBOUND_ROUTING_KEY : reboundTag2
    };

    var sailor3 = {
        LISTEN_MESSAGES_ON : messagesQueue3,
        PUBLISH_MESSAGES_TO : exchangeName,
        DATA_ROUTING_KEY : messageTag4, // sailor3 sends messages with tag 4
        ERROR_ROUTING_KEY : errorTag3,
        REBOUND_ROUTING_KEY : reboundTag3
    };

    Q.all([

        assertExchange(exchangeName),

        assertQueue(messagesQueue1), // create queue for messages for step1
        assertQueue(messagesQueue2), // create queue for messages for step2
        assertQueue(messagesQueue3), // create queue for messages for step3

        assertReboundsQueue(reboundsQueue1, exchangeName, messageTag1), // create queue for rebounds of step1
        assertReboundsQueue(reboundsQueue2, exchangeName, messageTag2), // create queue for rebounds of step2
        assertReboundsQueue(reboundsQueue3, exchangeName, messageTag3), // create queue for rebounds of step3

        subscribeQueueToKey(messagesQueue1, exchangeName, messageTag1), // queue1 should listen for messages for step1
        subscribeQueueToKey(messagesQueue2, exchangeName, messageTag2), // queue2 should listen for messages for step2
        subscribeQueueToKey(messagesQueue3, exchangeName, messageTag3), // queue3 should listen for messages for step3

        subscribeQueueToKey(reboundsQueue1, exchangeName, reboundTag1), // rebounds1 should listen for reboundTag1
        subscribeQueueToKey(reboundsQueue2, exchangeName, reboundTag2), // rebounds2 should listen for reboundTag2
        subscribeQueueToKey(reboundsQueue3, exchangeName, reboundTag3) // rebounds3 should listen for reboundTag3

    ]).then(function(){
        console.log('Done!');
    });



});