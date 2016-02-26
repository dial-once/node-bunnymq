var producer = require('../../index')().producer;

producer.connect()
.then(function (_channel) {
  producer.produce('redeliverQueue', { message: 'message' });
});
