var Q = require('q');
var util = require('util');
var _ = require('lodash');

/**
 * queueCreator.makeQueuesForTheTask(task, execId) returns JSON object which contains .env vars for sailor for each of steps
 * {"0": {...}, "1": {...}, "2": {....}} where 0,1,2 is step number in the flow
 * and where each {...} contains TASK, STEP_ID, LISTEN_MESSAGES_ON, PUBLISH_MESSAGES_TO, ERROR_ROUTING_KEY, REBOUND_ROUTING_KEY, DATA_ROUTING_KEY
 */

exports.QueueCreator = QueueCreator;

function QueueCreator(channel){

    var REBOUND_QUEUE_TTL = 10 * 60 * 1000; // 10 min

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

                var resultTag = getRoutingTag(task.id, stepId, execId, "result"); // step1.result (outgoing msg)
                var errorTag   = getRoutingTag(task.id, stepId, execId, "error"); // step1.error (error)
                var reboundTag = getRoutingTag(task.id, stepId, execId, "rebound"); // step1.rebound (rebound)
                var requeueTag   = getRoutingTag(task.id, stepId, execId, "requeue"); // step1.requeue (requeued messages)

                // create queues for messages, errors, rebounds
                stepPromises.push(assertQueue(messagesQueue));
                stepPromises.push(assertQueue(errorsQueue));
                stepPromises.push(assertReboundsQueue(reboundsQueue, exchangeName, requeueTag)); // return rebounds to messages

                // subscribe queues for their tags
                stepPromises.push(subscribeQueueToKey(messagesQueue, exchangeName, requeueTag)); // listen requeued messages
                stepPromises.push(subscribeQueueToKey(errorsQueue, exchangeName, errorTag)); // listen errors
                stepPromises.push(subscribeQueueToKey(reboundsQueue, exchangeName, reboundTag)); // listen rebounds

                // subscribe messages queue for results from previous step
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
                return Q(stepsSailorSettings);
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
        return channel.bindQueue(queueName, exchangeName, routingKey).then(function assertQueueSuccess() {
            console.log('Send keys %s to queue %s ', routingKey,  queueName);
        });
    }

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

    this.makeQueuesForTheTask = makeQueuesForTheTask;
}
