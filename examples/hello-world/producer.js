var producer = require('../../index')({ amqpAttempt: 3 }).producer;

producer.connect()
.then(function (_channel) {
  producer.produce('queueName', { message: 'hello world!' });
});
