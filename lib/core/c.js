const BunnyMq = require('./bunnymq');

const bunnyMq = new BunnyMq({ debug: true, host: 'amqp://localhost', timeout: 3000 });

bunnyMq.subscribe('testttt', '', 'routing', {}, (msg, info) => {
  console.log(bunnyMq.bmqConnectionsPool);
  console.log('message:', msg);
  console.log('info:', info);
  // throw new Error('test');
});

bunnyMq.subscribe('testttt', '', 'routing', {}, (msg, info) => {
  console.log(bunnyMq.bmqConnectionsPool);
  console.log('message:', msg);
  console.log('info:', info);
  // throw new Error('test');
});

bunnyMq.subscribe('testttt', '', 'routing', {}, (msg, info) => {
  console.log(bunnyMq.bmqConnectionsPool);
  console.log('message:', msg);
  console.log('info:', info);
  // throw new Error('test');
});
