const parsers = require('./message-parsers');
const utils = require('./utils');
const assert = require('assert');
const uuid = require('node-uuid');
const Deferred = require('../classes/deferred');

const ERRORS = {
  TIMEOUT: 'Timeout reached',
  BUFFER_FULL: 'Buffer is full'
};

class Producer {
  constructor(connection) {
    this.amqpRPCQueues = {};
    this._connection = connection;
    this.channel = null;
  }

  set connection(value) {
    this._connection = value;
  }

  /**
   * Get a function to execute on channel consumer incoming message is received
   * @param  {string} queue name of the queue where messages are SENT
   * @return {function}       function executed by an amqp.node channel consume callback method
   */
  maybeAnswer(queue) {
    const rpcQueue = this.amqpRPCQueues[queue];

    return (msg) => {
      try {
        // check the correlation ID sent by the initial message using RPC
        const corrId = msg.properties.correlationId;
        // if we found one, we execute the callback and delete it because it will never be received again anyway
        rpcQueue[corrId].resolve(parsers.in(msg));
        this._connection.config.transport.info('bmq:producer', `[${queue}] < answer`);
        delete rpcQueue[corrId];
      } catch (e) {
        this._connection.config.transport.error(new Error(
          `Receiving RPC message from previous session: callback no more in memory. ${queue}`
        ));
      }
    };
  }

  /**
   * Create a RPC-ready queue
   * @param  {string} queue the queue name in which we send a RPC request
   * @return {Promise}       Resolves when answer response queue is ready to receive messages
   */
  createRpcQueue(queue) {
    this.amqpRPCQueues[queue] = this.amqpRPCQueues[queue] || {};

    const rpcQueue = this.amqpRPCQueues[queue];
    if (rpcQueue.queue) return Promise.resolve(rpcQueue.queue);

    // we create the callback queue using base queue name + appending config hostname and :res for clarity
    // ie. if hostname is gateway-http and queue is service-oauth, response queue will be service-oauth:gateway-http:res
    // it is important to have different hostname or no hostname on each module sending message or there will be conflicts
    const resQueue = `${queue}:${this._connection.config.hostname}:res`;
    rpcQueue.queue = this._connection.get().then(channel =>
      channel.assertQueue(resQueue, { durable: true, exclusive: true })
        .then((q) => {
          rpcQueue.queue = q.queue;

          // if channel is closed, we want to make sure we cleanup the queue so future calls will recreate it
          this._connection.addListener('close', () => { delete rpcQueue.queue; this.createRpcQueue(queue); });

          return channel.consume(q.queue, this.maybeAnswer(queue), { noAck: true });
        })
        .then(() => rpcQueue.queue)
      )
      .catch(() => {
        delete rpcQueue.queue;
        return utils.timeoutPromise(this._connection.config.timeout).then(() =>
          this.createRpcQueue(queue)
        );
      });

    return rpcQueue.queue;
  }

  publishOrSendToQueue(queue, msg, options) {
    if (!options.routingKey) {
      return this.channel.sendToQueue(queue, msg, options);
    }
    return this.channel.publish(queue, options.routingKey, msg, options);
  }

  /**
   * Start a timer to reject the pending RPC call if no answer is received within the given timeout
   * @param  {string} queue  The queue where the RPC request was sent
   * @param  {string} corrId The RPC correlation ID
   * @param  {number} time    The timeout in ms to wait for an answer before triggering the rejection
   * @return {void}         Nothing
   */
  prepareTimeoutRpc(queue, corrId, time) {
    const producer = this;
    setTimeout(() => {
      const rpcCallback = producer.amqpRPCQueues[queue][corrId];
      if (rpcCallback) {
        rpcCallback.reject(new Error(ERRORS.TIMEOUT));
        delete producer.amqpRPCQueues[queue][corrId];
      }
    }, time);
  }

  /**
   * Send message with or without rpc protocol, and check if RPC queues are created
   * @param  {string} queue   the queue to send `msg` on
   * @param  {any} msg     string, object, number.. anything bufferable/serializable
   * @param  {object} options contain rpc property (if true, enable rpc for this message)
   * @return {Promise}         Resolves when message is correctly sent, or when response is received when rpc is enabled
   */
  checkRpc(queue, msg, options) {
    const settings = Object.assign({}, options);
    // messages are persistent
    settings.persistent = true;

    if (settings.rpc) {
      return this.createRpcQueue(queue)
        .then(() => {
          // generates a correlationId (random uuid) so we know which callback to execute on received response
          const corrId = uuid.v4();
          settings.correlationId = corrId;
          // reply to us if you receive this message!
          settings.replyTo = this.amqpRPCQueues[queue].queue;

          if (this.publishOrSendToQueue(queue, msg, settings)) {
            // defered promise that will resolve when response is received
            const responsePromise = new Deferred();
            this.amqpRPCQueues[queue][corrId] = responsePromise;
            if (settings.timeout) {
              this.prepareTimeoutRpc(queue, corrId, settings.timeout);
            }
            return responsePromise.promise;
          }
          return Promise.reject(ERRORS.BUFFER_FULL);
        });
    }

    return this.publishOrSendToQueue(queue, msg, settings);
  }

  /**
   * @deprecated Use publish instead
   * Ensure channel exists and send message using `checkRpc`
   * @param  {string} queue   The destination queue on which we want to send a message
   * @param  {any} msg     Anything serializable/bufferable
   * @param  {object} options message options (persistent, durable, rpc, etc.)
   * @return {Promise}         checkRpc response
   */
   /* eslint prefer-rest-params: off */
  produce(queue, msg, options) {
    return this.publish(queue, msg, options);
  }

  /**
   * Ensure channel exists and send message using `checkRpc`
   * @param  {string} queue   The destination queue on which we want to send a message
   * @param  {any} msg     Anything serializable/bufferable
   * @param  {object} options message options (persistent, durable, rpc, etc.)
   * @return {Promise}         checkRpc response
   */
  publish(queue, msg, options) {
    // default options are persistent and durable because we do not want to miss any outgoing message
    // unless user specify it
    const settings = Object.assign({ persistent: true, durable: true }, options);
    let message = Object.assign({}, msg);
    return this._connection.get()
    .then((channel) => {
      this.channel = channel;

      // undefined can't be serialized/buffered :p
      if (!message) message = null;

      this._connection.config.transport.info('bmq:producer', `[${queue}] > `, msg);

      return this.checkRpc(queue, parsers.out(message, settings), settings);
    })
    .catch((err) => {
      if ([ERRORS.TIMEOUT, ERRORS.BUFFER_FULL].includes(err.message)) {
        throw err;
      }
      // add timeout between retries because we don't want to overflow the CPU
      this._connection.config.transport.error('bmq:producer', err);
      return utils.timeoutPromise(this._connection.config.timeout)
      .then(() => this.publish(queue, message, settings));
    });
  }
}

class Consumer {

  constructor(connection) {
    this._connection = connection;
    this.channel = null;
  }

  set connection(value) {
    this._connection = value;
  }
  /**
   * Get a function to execute on incoming messages to handle RPC
   * @param  {any} msg   An amqp.node message object
   * @param  {string} queue The initial queue on which the handler received the message
   * @return {function}       a function to use in a chaining on incoming messages
   */
  checkRpc(msg, queue) {
    /**
     * When message contains a replyTo property, we try to send the answer back
     * @param  {any} content the received message:
     * @return {any}          object, string, number... the current received message
     */
    return (content) => {
      if (msg.properties.replyTo) {
        const options = { correlationId: msg.properties.correlationId, persistent: true, durable: true };
        this._connection.config.transport.info('bmq:consumer', `[${queue}][${msg.properties.replyTo}] >`, content);
        this.channel.sendToQueue(msg.properties.replyTo, parsers.out(content, options), options);
      }

      return msg;
    };
  }

  /**
   * @deprecated use subscribe instead
   * Create a durable queue on RabbitMQ and consumes messages from it - executing a callback function.
   * Automaticaly answers with the callback response (can be a Promise)
   * @param  {string}   queue    The RabbitMQ queue name
   * @param  {object}   options  (Optional) Options for the queue (durable, persistent, etc.)
   * @param  {Function} callback Callback function executed when a message is received on the queue name, can return a promise
   * @return {Promise}           A promise that resolves when connection is established and consumer is ready
   */
   /* eslint no-param-reassign: "off" */

  consume(queue, options, callback) {
    return this.subscribe(queue, options, callback);
  }

  subscribe(queue, options, callback) {
    if (typeof options === 'function') {
      callback = options;
      // default message options
      options = { persistent: true, durable: true };
    }

    // consumer gets a suffix if one is set on the configuration, to suffix all queues names
    // ex: service-something with suffix :ci becomes service-suffix:ci etc.
    const suffixedQueue = queue + this._connection.config.consumerSuffix;

    return this._connection.get()
    .then((channel) => {
      this.channel = channel;

      // when channel is closed, we want to be sure we recreate the queue ASAP so we trigger a reconnect by recreating the consumer
      this.channel.addListener('close', () => {
        this.consume(queue, options, callback);
      });

      return this.channel.assertQueue(suffixedQueue, options)
      .then((q) => {
        this._connection.config.transport.info('bmq:consumer', 'init', q.queue);

        this.channel.consume(q.queue, (msg) => {
          this._connection.config.transport.info('bmq:consumer', `[${q.queue}] < ${msg.content.toString()}`);

          // main answer management chaining
          // receive message, parse it, execute callback, check if should answer, ack/reject message
          Promise.resolve(parsers.in(msg))
          .then(callback)
          .then(this.checkRpc(msg, q.queue))
          .then(() => {
            this.channel.ack(msg);
          })
          .catch((err) => {
            // if something bad happened in the callback, reject the message so we can requeue it (or not)
            this._connection.config.transport.error('bmq:consumer', err);
            this.channel.reject(msg, this._connection.config.requeue);
          });
        }, { noAck: false });

        return true;
      });
    })
    .catch(() =>
      // in case of any error creating the channel, wait for some time and then try to reconnect again (to avoid overflow)
      utils.timeoutPromise(this._connection.config.timeout)
        .then(() => this.subscribe(queue, options, callback))
    );
  }
}

class BunnyMQ {
  constructor(connection) {
    this._connection = connection;
    this.producer = new Producer(connection);
    this.consumer = new Consumer(connection);
  }

  get connection() {
    return this._connection;
  }

  set connection(value) {
    this._connection = value;
    this.producer.connection = value;
    this.consumer.connection = value;
  }

  // for backward compatibility. @deprecated
  consume(queue, options, callback) {
    return this.consumer.subscribe(queue, options, callback);
  }

  subscribe(queue, options, callback) {
    return this.consumer.subscribe(queue, options, callback);
  }

  // for backward compatibility. @deprecated
  produce(queue, msg, options) {
    return this.producer.publish(queue, msg, options);
  }

  publish(queue, msg, options) {
    return this.producer.publish(queue, msg, options);
  }
}

let instance;
module.exports = (connection) => {
  assert(instance || connection, 'BunnyMQ can not be initialized because connection does not exist');

  if (!instance) {
    instance = new BunnyMQ(connection);
  } else {
    instance.connection = connection;
  }
  return {
    consume: instance.consume.bind(instance),
    subscribe: instance.subscribe.bind(instance),
    produce: instance.produce.bind(instance),
    publish: instance.publish.bind(instance),
    connection: instance.connection
  };
};
