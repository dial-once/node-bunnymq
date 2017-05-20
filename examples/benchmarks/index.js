const Benchmark = require('benchmark');
const { producer, consumer, connection } = require('../../index')({
  hostname: process.env.HOSTNAME || 'consumer',
  poolSize: parseInt(process.env.AMQP_POOL_SIZE, 10) || 5000,
  timeout: parseInt(process.env.AMQP_TIMEOUT, 10) || 1000,
  rpcTimeout: parseInt(process.env.AMQP_RPC_TIMEOUT, 10) || 2000,
  host: process.env.AMQP_URL || 'amqp://localhost'
});

const LOOP_INTERVAL = parseInt(process.env.LOOP_INTERVAL, 10) || 10;
const MAX_MESSAGES = parseInt(process.env.MAX_MESSAGES, 10) || 100;
const suite = new Benchmark.Suite;

process.on('unhandledRejection', error => {
  // Will print "unhandledRejection err is not dewfined"
  console.error('unhandledRejection', error);
});

connection.connect()
.then(() => {
  consumer.consume('test:queue', (msg) => {
    // console.log(msg);
    return Promise.resolve(true);
  })
  .then(() => {
    // add tests
    suite
    .add('with RPC', {
      defer: true,
      repeat: 10,
      fn: function (deferred) {
        // avoid test inlining
        producer.produce('test:queue', { message: 'message', duration: 1000 }, { rpc: true })
        .then(() => deferred.resolve())
        .catch(() => deferred.resolve());
      }
    })
    .add('without RPC', {
      defer: true,
      repeat: 10,
      fn: function (deferred) {
        // avoid test inlining
        producer.produce('test:queue', { message: 'message', duration: 1000 }, { rpc: false })
        .then(() => deferred.resolve())
        .catch(() => deferred.resolve());
      }
    })
    // // add listeners
    .on('cycle', function (event) {
      console.log(String(event.target));
    })
    .on('complete', function () {
      console.log('Fastest is ' + this.filter('fastest').map('name'));
    })
    // run async
    .run({ 'async': true });
  });
});
