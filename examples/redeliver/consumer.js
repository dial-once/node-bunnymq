var consumer = require('../../index')({ redeliverTTL: 3 }).consumer;

consumer.connect()
.then(function (_channel) {
  consumer.consume('redeliverQueue', function (_msg) {
    throw Error('Any kind of error');
  });
});
