var assert = require('assert');
var producer = require('../src/index')().producer;
var consumer = require('../src/index')().consumer;
var utils = require('../src/modules/utils');
var uuid = require('node-uuid');
var docker = require('./docker');

var fixtures = {
  queues: ['test-queue-0', 'test-queue-1', 'test-queue-2', 'test-queue-3', 'test-queue-4'],
  routingKey: 'queue-routing-key'
};

var letters = 0;

describe('producer/consumer', function () {
  before(function () {
    this.timeout(20000);
    return docker.start();
  });

  after(() => {
    return docker.stop();
  });

  describe('msg delivering', function () {
    before(() => {
      return consumer.consume(fixtures.queues[0], function () {
        letters--;
      }).then(() => {
        return consumer.consume(fixtures.queues[1], function () {
          letters--;
        });
      });
    });

    it('should be able to consume message sent by producer to queue [test-queue-0]', function () {
      letters++;
      return producer.produce(fixtures.queues[0], {msg: uuid.v4()})
        .then(() => utils.timeoutPromise(300))
        .then(() => assert.equal(letters, 0));
    });

    it('should be able to consume message sent by producer to queue [test-queue-0] (no message)', function () {
      letters++;
      return producer.produce(fixtures.queues[0])
        .then(() => utils.timeoutPromise(300))
        .then(() => assert.equal(letters, 0));
    });

    it('should be able to consume message sent by producer to queue [test-queue-0] (null message)', function () {
      letters++;
      return producer.produce(fixtures.queues[0], null)
        .then(() => utils.timeoutPromise(300))
        .then(() => assert.equal(letters, 0));
    });

    it('should not be able to consume message sent by producer to queue [test-queue-1]', function () {
      letters++;
      return producer.produce(fixtures.queues[1], null)
        .then(() => utils.timeoutPromise(300))
        .then(() => assert.equal(letters, 0));
    });

    it('should be able to consume all message populated by producer to all queues [test-queue-0, test-queue-1, test-queue-2]', function () {
      var count = 100;
      var messages = [];
      letters += 200;

      for (let i = 0; i < count; i++) {
        messages.push(producer.produce(fixtures.queues[0], null));
        messages.push(producer.produce(fixtures.queues[1], null));
      }

      return Promise.all(messages)
        .then(() => utils.timeoutPromise(500))
        .then(() => assert.equal(letters, 0));
    });

  });

  describe('msg requeueing', function () {
    it('should be able to consume message, but throw error so the message is requeued again on queue [test-queue-0]', function (done) {
      var attempt = 3;

      consumer.consume(fixtures.queues[3], function (_msg) {
          assert(typeof _msg === 'object');

          --attempt;
          if (!attempt) {
            return done();
          } else {
            throw new Error('Any kind of error');
          }
        })
        .then(function () {
          producer.produce(fixtures.queues[3], {msg: uuid.v4()})
            .then(function (response) {
              assert(response === true);
              ++letters;
            });
        });
    });
  });

  describe('producer/no queue', function () {
    it('should reject for rpc channel in case the queue does not exists', function (done) {
      var fakeQueue = 'fake:queue:name';
      producer.produce(fakeQueue, {msg: Date.now()}, {rpc: true})
        .then(function () {
          done(new Error('it should reject'));
        })
        .catch(function (err) {
          try {
            assert(err instanceof Error);
            assert(err.message.indexOf('NOT_FOUND') > -1);
            assert(err.message.indexOf('404') > -1);
            assert(err.message.indexOf(fakeQueue) > -1);

            // check if everything is working after the no name queue
            return consumer.consume(fixtures.queues[4], (_msg) =>  assert(_msg))
              .then(() => {
                return producer.produce(fixtures.queues[4], {msg: Date.now()}, {rpc: true});
              })
              .then(() => {
                done();
              })
              .catch(done);

          } catch (e) {
            done(e);
          }
        });
    });

    it('should reject for non rpc channel in case the queue does not exists', function (done) {
      var fakeQueue = 'fake:queue:name';
      producer.produce(fakeQueue, {msg: Date.now()}, {rpc: false})
        .then(function () {
          done(new Error('it should reject'));
        })
        .catch(function (err) {
          try {
            assert(err instanceof Error);
            assert(err.message.indexOf('NOT_FOUND') > -1);
            assert(err.message.indexOf('404') > -1);
            assert(err.message.indexOf(fakeQueue) > -1);
            done();
          } catch (e) {
            done(e);
          }
        });
    });
  });

  describe('routing keys', function () {
    it('should be able to send a message to a routing key exchange', function () {
      return consumer.consume(fixtures.routingKey, function (message) {
          assert.equal(message.content, 'ok');
        })
        .then(() => {
          return producer.produce(fixtures.routingKey, {content: 'ok'}, {routingKey: 'route'});
        });
    });
  });
});
