require('events').EventEmitter.prototype._maxListeners = process.env.MAX_EMITTERS || 20;

const Connection = require('./lib/connection');
const Consumer = require('./lib/consumer');
const Producer = require('./lib/producer');
const uuid = require('uuid');
const debug = require('debug');

let connection;

module.exports = (config = {}) => {
  const configuration = Object.assign({
    host: 'amqp://localhost',
    // number of fetched messages, at once
    prefetch: 5,
    // requeue put back message into the broker if consumer crashes/trigger exception
    requeue: true,
    // requeue count, after this number of retry the message will be rejected
    requeueCount: 5,
    //  time between two reconnect (ms)
    timeout: 1000,
    //  default timeout for RPC calls. If set to '0' there will be none.
    rpcTimeout: 15000,
    consumerSuffix: '',
    // generate a hostname so we can track this connection on the broker (rabbitmq management plugin)
    hostname: process.env.HOSTNAME || process.env.USER || uuid.v4(),
    enableCompression: process.env.AMQP_ENABLE_COMPRESSION === 'true'
    // the transport to use to debug. if provided, bunnymq will show some logs
    // transport: utils.emptyLogger
  }, config);

  if (!connection) {
    connection = new Connection(configuration, debug('[BunnyMq:connection]'));
  }

  return {
    consumer: new Consumer(connection, configuration, debug('[BunnyMq:consumer]')),
    producer: new Producer(connection, configuration, debug('[BunnyMq:producer]')),
    connection,
  };
};
