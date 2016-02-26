var consumer = require('../../index')().consumer;

consumer.connect()
.then(function (_channel) {
  consumer.consume('queueName', function (_msg) {
    return true;
  });
});
