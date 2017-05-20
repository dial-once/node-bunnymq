const { consumer, producer, connection } = require('../../../index')({
  hostname: process.env.HOSTNAME || 'consumer',
  poolSize: parseInt(process.env.AMQP_POOL_SIZE, 10) || 100,
  timeout: parseInt(process.env.AMQP_TIMEOUT, 10) || 1000,
  host: process.env.AMQP_URL || 'amqp://localhost',
  prefetch: 100,
  enableCompression: true
});

connection.connect()
.then(() => {
  consumer.consume('queue:heartbeat', (msg) => Promise.resolve(true));
  consumer.consume('queue:check-permissions', (msg) => Promise.resolve(true));
  consumer.consume('queue:update-paths', (msg) => Promise.resolve(true));
});
