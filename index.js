
// var uuid = require('node-uuid');

var defaultConfig = {
  isLogEnabled: true,
  amqpUrl: process.env.AMQP_URL || 'amqp://localhost',
  prefetch: process.env.AMQP_PREFETCH || 1,
  isRequeueEnabled: true,
  queues: [{ name: '', options: { exclusive: true, persistent: true } }],
  exchanges: [{ name: 'defaultExchange', type: 'topic', options: { durable: true, persistent: true } }],
  bindings: [{ exchange: 'defaultExchange', queue: '', routingKeys: ['test.', '*.key'] }]
};

module.exports = function(config) {
  return {
    producer: require('./lib/producer')(config || defaultConfig),
    consumer: require('./lib/consumer')(config || defaultConfig)
  };
};
