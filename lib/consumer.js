var amqp = require('./amqp-layer')();
var logger = require('../helpers/logger')(true);

var channel, config;
var connected = connecting = false;
var reqQueue = [];

function assertQueues() {
  var promises = [];
  var queue;

  for (var i = 0, l = config.queues.length; i < l; ++i) {
    queue = config.queues[i];
    promises.push(channel.assertQueue(queue.name, queue.options));
  }

  return Promise.all(promises);
}

function assertExchanges() {
  var promises = [];
  var exchange;

  for (var i = 0, l = config.exchanges.length; i < l; ++i) {
    exchange = config.exchanges[i];
    promises.push(channel.assertExchange(exchange.name, exchange.type, exchange.options));
  }

  return Promise.all(promises);
}

function bindQueues() {
  var bind;
  var routingKey;

  for (var i = 0, l = config.bindings.length; i < l; ++i) {
    bind = config.bindings[i];
    for (var j = 0, k = bind.routingKeys.length; j < k; ++j) {
      routingKey = bind.routingKeys[j];
      console.log('exchange:', bind.exchange);
      console.log('key:', routingKey);
      channel.bindQueue(bind.queue, bind.exchange, routingKey);
    }
  }
}

function start(done) {
  channel.prefetch(config.prefetch);

  return assertExchanges()
  .then(function () {
    return assertQueues();
  })
  .then(function (_q) {
    bindQueues();
    for (var i = 0, l = _q.length; i < l; ++i) {
      console.log('queue:', _q[i].queue);
      channel.consume(_q[i].queue, done, { noAck: false });
    }
  });
}

var connect = function(_amqpUrl) {
  connecting = true;

  return amqp.connect(_amqpUrl || config.amqpUrl)
  .then(function (_channel) {
    logger.log('info', '[AMQP] Consumer is now connected and ready to consume messages');

    channel = _channel;
    connected = true;

    while (reqQueue.length > 0) {
      reqQueue.pop()();
    }

    return channel;
  })
  .catch(function (_err) {
    logger.log('error', '[AMQP] Consumer ', _err.stack, '\n    occured while connecting');
    amqp.reconnect();
  });
};

var consume = function(_callback) {
  if (!connected) {
    reqQueue.push(function () {
      consume(_callback);
    });

    if (!connecting) {
      connect();
    }
  } else {
    var doneCallback = function (_msg) {
      var strMsg = _msg.content.toString();
      logger.log('info', '[AMQP] Consumer received msg:', strMsg);

      return Promise.resolve(strMsg)
      .then(_callback)
      .then(function (_response) {
        channel.ack(_msg);
      })
      .catch(function (_err) {
        logger.log('error', '[AMQP] Consumer ', _err.stack, '\n    occured while processing msg:', strMsg);
        if (_err) channel.reject(_msg, config.isRequeueEnabled);
      });
    };

    start(doneCallback);
  }
};

module.exports = function (_config) {
  config = _config;

  logger.enableDisableLog(config.isLogEnabled);

  return {
    connect: connect,
    disconnect:amqp.disconnect,
    consume: consume
  };
};
