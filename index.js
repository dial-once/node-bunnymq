require('./lib/boot/logger');

var defaultConfig = {
  host: process.env.AMQP_URL || 'amqp://localhost',
  prefetch: process.env.AMQP_PREFETCH || 1,
  redeliverTTL: 10
};

module.exports = function(config) {
  var tmpConfig = defaultConfig;

  if (config) {
    tmpConfig = {
      host: config.host || config.amqpUrl || defaultConfig.host,
      prefetch: config.prefetch || config.amqpPrefetch || defaultConfig.prefetch,
      redeliverTTL: config.redeliverTTL || config.amqpRedeliverTTL || defaultConfig.redeliverTTL
    };
  }

  return {
    producer: require('./lib/producer')(tmpConfig),
    consumer: require('./lib/consumer')(tmpConfig)
  };
};
