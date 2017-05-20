const Connection = require('./connection');
const Message = require('./message');
const utils = require('./modules/utils');
const uuid = require('uuid');

class Producer {
  constructor(connection, config, debug) {
    this.connection = connection;
    this.rpcQueues = {};
    this.config = config;
    this.debug = debug;
    // method binding
    this.checkAnswer = this.checkAnswer.bind(this);
    this.checkRPCTimeout = this.checkRPCTimeout.bind(this);
    this.createRpcQueue = this.createRpcQueue.bind(this);
    this.checkRPC = this.checkRPC.bind(this);
    this.produce = this.produce.bind(this);
    this.produceWhenConnected = this.produceWhenConnected.bind(this);
  }

  static get ERRORS() {
    return {
      TIMEOUT: 'Timeout reached',
      PREVIOUS_SESSION: 'Receiving previous session RPC message: callback no more in memory'
    };
  };

  checkAnswer(channel, queue) {
    return (msg) => {
      // check the correlation ID sent by the initial message using RPC
      const rpcPromise = this.rpcQueues[queue][msg.properties.correlationId];

      this.connection.releaseChannel(channel);

      return Message.deserialize(msg.content, msg.fields)
      .then((deserializedMsg) => {
        this.debug(`queue=${queue} < `, deserializedMsg);

        if (rpcPromise && typeof rpcPromise.resolve === 'function') {
          rpcPromise.resolve(deserializedMsg);
          delete this.rpcQueues[queue][msg.properties.correlationId];
        }
      });
    };
  }

  checkRPCTimeout(channel, queue, corrId, rpcTimeout) {
    setTimeout(() => {
      const rpcPromise = this.rpcQueues[queue][corrId];
      if (rpcPromise) {
        rpcPromise.reject(new Error(Producer.ERRORS.TIMEOUT));
        delete this.rpcQueues[queue][corrId];
        this.connection.releaseChannel(channel);
      }
    }, rpcTimeout);
  }

  publishOrSendToQueue(channel, queue, msg, options) {
    const message = new Message({
      body: msg,
      correlationId: options.correlationId,
      replyTo: options.replyTo,
      // requeue: options.requeue,
      // requeueCount: options.requeueCount
    });

    return Message.serialize(message, options.enableCompression)
    .then((serializedMsg) => {
      this.debug(`Queueing message in queue=${queue} > `, message);

      options.headers = { requeueCount: options.requeueCount, requeue: options.requeue };

      if (!options.routingKey) return channel.sendToQueue(queue, serializedMsg, options);

      return channel.publish(queue, options.routingKey, serializedMsg, options);
    });
  }

  createRpcQueue(channel, queue) {
    this.rpcQueues[queue] = this.rpcQueues[queue] || {};

    const rpcQueue = this.rpcQueues[queue];

    if (rpcQueue.queue) return Promise.resolve(rpcQueue.queue);

    // we create the callback queue using base queue name + appending config hostname and :res for clarity
    // ie. if hostname is gateway-http and queue is service-oauth, response queue will be service-oauth:gateway-http:res
    // it is important to have different hostname or no hostname on each module sending message or there will be conflicts
    const resQueue = `${queue}:${this.config.hostname || uuid.v4()}:res`;

    return channel.assertQueue(resQueue, { durable: false, exclusive: true })
    .then((q) => {
      rpcQueue.queue = q.queue;
      // if channel is closed, we want to make sure we cleanup the queue so future calls will recreate it
      channel.on('error', () => {
        delete rpcQueue.queue;
        this.createRpcQueue(channel, queue);
      });

      return channel.consume(q.queue, this.checkAnswer(channel, queue), { noAck: true });
    })
    .then(() => rpcQueue.queue)
    .catch(() => {
      delete rpcQueue.queue;
      return utils.timeoutPromise(this.config.timeout)
      .then(() => this.createRpcQueue(channel, queue));
    });
  }

  checkRPC(channel, queue, msg, options) {
    if (!options.rpc) return this.publishOrSendToQueue(channel, queue, msg, options);

    const correlationId = uuid.v4();
    const mergedOptions = Object.assign({ correlationId }, options);

    return this.createRpcQueue(channel, queue)
    .then(() => {
      mergedOptions.replyTo = this.rpcQueues[queue].queue;

      return this.publishOrSendToQueue(channel, queue, msg, mergedOptions)
      .then(() => {
        // defered promise that will resolve when response is received
        const rpcPromise = new utils.Deferred();
        this.rpcQueues[queue][correlationId] = rpcPromise;

        if (mergedOptions.rpcTimeout) {
          this.checkRPCTimeout(channel, queue, correlationId, mergedOptions.rpcTimeout);
        }

        return rpcPromise.promise;
      });
    });
  }

  produceWhenConnected(channel, queue, msg, options) {
    // undefined can't be serialized/buffered :p
    const validMsg = msg || null;

    return this.checkRPC(channel, queue, validMsg, options)
    .then((res) => {
      this.connection.releaseChannel(channel);
      return res;
    })
    .catch((err) => {
      this.debug(err);

      if ([Producer.ERRORS.TIMEOUT].includes(err.message)) return Promise.reject(err);

      // add timeout between retries because we don't want to overflow the CPU
      return utils.timeoutPromise(this.config.timeout)
      .then(() => this.produce(queue, validMsg, options));
    });
  }

  produce(queue, msg, options) {
    return new Promise((resolve, reject) => {
      // default options are persistent and durable because we do not want to miss any outgoing message
      const mergedOptions = Object.assign({ persistent: true, durable: true }, this.config, options);

      this.connection.eventEmitter.once('connected', () => {
        this.connection.getChannel()
        .then((channel) => {
          channel.prefetch(mergedOptions.prefetch);

          this.produceWhenConnected(channel, queue, msg, mergedOptions)
          .then(resolve)
          .catch(reject);
        })
        .catch((err) => {
          if (err.msg === 'Channel pool is empty') throw err;
        });
      });

      this.connection.connect();
    });
  }
}

module.exports = Producer;
