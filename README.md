# sailor-nodejs

The official elastic.io library for bootstrapping and executing for Node.js connectors.

Minimal .env vars required:

ELASTICIO_TASK={"id":"5559edd38968ec0736000003","data":{"step_1":{"uri":"546456456456456"}},"recipe":{"nodes":[{"id":"step_1","function":"passthrough"}]}} <br/>
ELASTICIO_STEP_ID=step_1<br/>
<br/>
ELASTICIO_AMQP_URI=amqp://guest:guest@localhost:5672<br/>
<br/>
ELASTICIO_LISTEN_MESSAGES_ON=5559edd38968ec0736000003:step_1:1432205514864:messages<br/>
ELASTICIO_PUBLISH_MESSAGES_TO=userexchange:5527f0ea43238e5d5f000001<br/>
ELASTICIO_DATA_ROUTING_KEY=5559edd38968ec0736000003:step_1:1432205514864:message<br/>
ELASTICIO_ERROR_ROUTING_KEY=5559edd38968ec0736000003:step_1:1432205514864:error<br/>
ELASTICIO_REBOUND_ROUTING_KEY=5559edd38968ec0736000003:step_1:1432205514864:rebound<br/>
<br/>

Optional .env vars and their default values:

ELASTICIO_COMPONENT_PATH=../ <br/>
ELASTICIO_REBOUND_INITIAL_EXPIRATION=15000 <br/>
ELASTICIO_REBOUND_LIMIT=2 <br/>


