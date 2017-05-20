const { producer, connection } = require('../../../index')({
  hostname: process.env.HOSTNAME || 'consumer',
  poolSize: parseInt(process.env.AMQP_POOL_SIZE, 10) || 5000,
  timeout: parseInt(process.env.AMQP_TIMEOUT, 10) || 1000,
  rpcTimeout: parseInt(process.env.AMQP_RPC_TIMEOUT, 10) || 15000,
  host: process.env.AMQP_URL || 'amqp://localhost',
  enableCompression: true
});

const LOOP_INTERVAL = parseInt(process.env.LOOP_INTERVAL, 10) || 1000;
const MAX_MESSAGES = parseInt(process.env.MAX_MESSAGES, 10) || 100;

connection.connect()
.then(() => {
  setInterval(() => {
    for (let i = 0; i < MAX_MESSAGES; ++i) {
      producer.produce('queue:heartbeat', { message: `message`, duration: 1000 }, { rpc: true })
      .then((res1) => producer.produce('queue:check-permissions', res1, { rpc: true }))
      .then((res2) => producer.produce('queue:update-paths', res2, { rpc: true }))
      .catch(console.error);
    }
  }, LOOP_INTERVAL);
});
