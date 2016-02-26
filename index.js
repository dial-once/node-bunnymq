require('./lib/boot/logger');

/* jshint maxcomplexity: false */
module.exports = function(config) {
  var defaultConfig = {
    host: process.env.AMQP_URL || 'amqp://localhost',
    prefetch: process.env.AMQP_PREFETCH || 1,
    redeliverTTL: 10
  };

  if (config) {
    defaultConfig = {
      host: config.host || config.amqpUrl || defaultConfig.host,
      prefetch: config.prefetch || config.amqpPrefetch || defaultConfig.prefetch,
      redeliverTTL: config.redeliverTTL || config.amqpRedeliverTTL || defaultConfig.redeliverTTL
    };
  }

  defaultConfig.prefetch = (typeof defaultConfig.prefetch !== 'number') ? parseInt(defaultConfig.prefetch) : defaultConfig.prefetch;

  return {
    producer: require('./lib/producer')(defaultConfig),
    consumer: require('./lib/consumer')(defaultConfig)
  };
};
