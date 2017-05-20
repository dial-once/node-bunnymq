const utils = require('./modules/utils');
const Connection = require('./connection');
const Message = require('./message');

process.on('unhandledRejection', console.error);

class Consumer {
  constructor(connection, config, debug) {
    this.connection = connection;
    this.config = config;
    this.debug = debug;
    // methods binding
    this.checkRPC = this.checkRPC.bind(this);
    this.consume = this.consume.bind(this);
    this.consumeWhenConnected = this.consumeWhenConnected.bind(this);
    this.reject = this.reject.bind(this);
  }

  checkRPC(channel, queue, msg, options) {
    /**
     * When message contains a replyTo property, we try to send the answer back
     * @param  {any} content the received message:
     * @return {any}          object, string, number... the current received message
     */
    return (content) => {
      if (msg.replyTo) {
        const message = new Message(Object.assign(msg, {
          body: content,
          from: msg.to,
          to: msg.from,
          requeue: options.requeue,
          requeueCount: options.requeueCount
        }));

        return Message.serialize(message, options.enableCompression)
        .then((serializedMsg) => {
          this.debug(`Queueing message in queue=${queue} rpcQueue=${msg.replyTo} > `, message);

          return channel.sendToQueue(msg.replyTo, serializedMsg, {
            correlationId: msg.correlationId,
            persistent: true,
            durable: true
          });
        });
      }

      return msg;
    };
  }

  consumeWhenConnected(queue, options, callback) {
    return this.connection.getChannel()
    .then((channel) => {
      // consumer gets a suffix if one is set on the configuration, to suffix all queues names
      // ex: service-something with suffix :ci becomes service-suffix:ci etc.
      const suffixedQueue = `${queue}${this.config.consumerSuffix || ''}`;

      channel.prefetch(options.prefetch);

      return channel.assertQueue(suffixedQueue, options)
      .then((q) => ({ validQueue: q.queue, channel }));
    })
    .then(({ validQueue, channel }) => {
      this.debug(`init queue=${validQueue}`);

      channel.consume(validQueue, (msg) => {
        let deserializedMsg;
        // main answer management chaining
        // receive message, parse it, execute callback, check if should answer, ack/reject message
        Message.deserialize(msg.content, msg.fields)
        .then((resMsg) => {
          deserializedMsg = resMsg;

          this.debug(`Receiving message in queue=${validQueue} < `, deserializedMsg);

          return Promise.resolve(callback(deserializedMsg))
          .then(this.checkRPC(channel, validQueue, deserializedMsg, options))
          .then(() => channel.ack(msg))
        })
        .catch((err) => {
          // if something bad happened in the callback, reject the message so we can requeue it (or not)
          this.debug(err);
          return this.reject(channel, queue, deserializedMsg, msg, options);
        });
      }, { noAck: false });

      return true;
    })
    .catch((err) => {
      this.debug(err);

      // add timeout between retries because we don't want to overflow the CPU
      return utils.timeoutPromise(this.config.timeout)
      .then(() => this.consume(queue, options, callback))
    });
  }

  reject(channel, queue, deserializedMsg, msg, options) {
    const headers = msg.properties.headers;
    headers.requeueCount -= 1;

    channel.reject(msg, false);
    if (headers.requeue && parseInt(headers.requeueCount, 10) >= 0) {
      const message = new Message(deserializedMsg);
      message.redelivered = true;

      return Message.serialize(message, options.enableCompression)
      .then((serializedMsg) => {
        options.headers = { requeueCount: headers.requeueCount, requeue: headers.requeue };

        this.debug(`Requeueing message in queue=${queue} > `, message);

        channel.sendToQueue(queue, serializedMsg, options);
      });
    }
  }

  consume(queue, options, callback) {
    return new Promise((resolve, reject) => {
      let currentOptions = options;
      let currentCallback = callback;

      if (typeof currentOptions === 'function') {
        currentCallback = currentOptions;
        // default message options
        currentOptions = { persistent: true, durable: true };
      }

      currentOptions = Object.assign({}, this.config, currentOptions);

      if (this.connection.state !== Connection.STATES.CONNECTED) {
        this.connection.eventEmitter.once('connected', (connection) => {
          this.consumeWhenConnected(queue, currentOptions, currentCallback)
          .then(resolve)
          .catch(reject);
        });
        this.connection.connect();
      } else {
        this.consumeWhenConnected(queue, currentOptions, currentCallback)
        .then(resolve)
        .catch(reject);
      }
    });
  }
}

module.exports = Consumer;
