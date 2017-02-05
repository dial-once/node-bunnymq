/* eslint-disable */

const amqp = require('amqplib');
const assert = require('assert');
const packageVersion = require('../../package.json').version;

let instance = null;

class BunnyMq {
  constructor(config) {
    if (!instance) {
      instance = this;

      this.bmqConfig = config;
      this.bmqConnection = null;
      this.bmqChannels = {};
      this.bmqStartedAt = new Date().toISOString();
      // bind methods
      this.connect = this.connect.bind(this);
    }

    return instance;
  }

  get config() {
    return this.bmqConfig;
  }

  set config(value) {
    this.bmqConfig = value;
  }

  get connection() {
    return this.bmqConnection;
  }

  set connection(value) {
    this.bmqConnection = value;
  }

  connect() {
    if (this.bmqConnection) return Promise.resolve(this.bmqConnection);

    return amqp.connect(this.bmqConfig.hostname, {
      clientProperties: {
        hostname: this.bmqConfig.hostname,
        bunnymq: packageVersion,
        startedAt: this.bmqStartedAt,
        connectedAt: new Date().toISOString()
      }
    })
    .then((connection) => {
      this.bmqConnection = connection;

      connection.on('close', () => { this.bmqConnection = null; });
      connectionn.on('error', this.bmqConfig.transport.error);

      return connection;
    })
    .catch((err) => {
      this.bmqConnection = null;
      throw err;
    });
  }

  subscribe(queue, options, callback) {
    if (typeof options === 'function') {
      callback = options;
      // default message options
      options = { persistent: true, durable: true };
    }

    // consumer gets a suffix if one is set on the configuration, to suffix all queues names
    // ex: service-something with suffix :ci becomes service-suffix:ci etc.
    const suffixedQueue = queue + this.bmqConfig.consumerSuffix;
    const prefetch = this.bmqConfig.prefetch;

    return this.connect()
    .then((connection) => connection.createChannel())
    .then((channel) => {
      this.bmqChannels[queue] = channel;

      channel.prefetch(prefetch);

      // on error we remove the channel so the next call will recreate it (auto-reconnect are handled by connection users)
      channel.on('close', () => { this.bmqChannels[queue] = null; });
      channel.on('error', this.bmqConfig.transport.error);

      return channel.assertQueue(suffixedQueue, options);
    })
    .then((q) => {
      this.bmqConfig.transport.info('bmq:consumer', 'init', q.queue);

      this.bmqChannels[q.queue].consume(q.queue, (msg) => {
        this.bmqConfig.transport.info('bmq:consumer', `[${q.queue}] < ${msg.content.toString()}`);

        // main answer management chaining
        // receive message, parse it, execute callback, check if should answer, ack/reject message
        Promise.resolve(parsers.in(msg))
        .then(callback)
        .then(this.checkRpc(msg, q.queue))
        .then(() => {
          this.bmqChannels[q.queue].ack(msg);
        })
        .catch((err) => {
          // if something bad happened in the callback, reject the message so we can requeue it (or not)
          this.bmqConfig.transport.error('bmq:consumer', err);
          this.bmqChannels[q.queue].reject(msg, this.bmqConfig.requeue);
        });
      }, { noAck: false });
    })
    .catch(() =>
      // in case of any error creating the channel, wait for some time and then try to reconnect again (to avoid overflow)
      utils.timeoutPromise(this.bmqConfig.timeout)
      .then(() => this.subscribe(queue, options, callback))
    );
  }

  // addListener(event, callback) {
  //   this.get().then((channel) => { channel.on(event, callback); });
  // }
}

module.exports = BunnyMq;
