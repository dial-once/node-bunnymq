const BunnyMq = require('./bunnymq');

const bunnyMq = new BunnyMq({ host: 'amqp://localhosdt', timeout: 5500, requeue: false });

bunnyMq.publish('testttt', 'routing', { message: 'hello world' }, {
  config: {
    host: 'amqp://localhost',
    hostname: 'coco',
    prefetch: 1,
    requeue: true,
    requeueLimit: 3,
    timeout: 1000,
    privateKey: undefined,
    debug: false
  },
  exchange: {
    type: 'direct',
    durable: true
  }
});
