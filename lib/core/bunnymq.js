/**
 * BunnyMq class
 */

// /* eslint-disable */
require('dotenv').load();
const uuid = require('uuid');
const amqp = require('amqplib');
const utils = require('../modules/utils');
const packageVersion = require('../../package.json').version;

/*
  * Setting up block level variable to store class state
  * , set's to null by default.
*/
let pInstance = null;

class BunnyMq {
  constructor(config) {
    if (!pInstance) {
      pInstance = this;

      this.bmqConfig = utils.setConfig(config);
      this.bmqStartedAt = new Date().toISOString();
      this.bmqConnectionsPool = {};

      // bind methods
      this.connect = this.connect.bind(this);
      this.publish = this.publish.bind(this);
      this.subscribe = this.subscribe.bind(this);
    }

    return pInstance;
  }

  get pool() {
    return this.bmqConnectionsPool;
  }

  get config() {
    return this.bmqConfig;
  }

  set config(value) {
    this.bmqConfig = value;
  }

  connect(config = {}) {
    // override instance config
    this.bmqConfig = Object.assign(this.bmqConfig, config);

    // set clientProperties to get more info about connection
    const clientProperties = {
      hostname: this.bmqConfig.hostname,
      bunnymq: packageVersion,
      startedAt: this.bmqStartedAt,
      connectedAt: new Date().toISOString(),
    };

    const poolId = uuid.v4();

    // open amqp connection
    return amqp.connect(this.bmqConfig.host, { clientProperties })
    .then((connection) => {
      const cnObj = Object.assign(connection);
      cnObj.poolId = poolId;

      utils.logger.info('[connect] - connection openned');

      // set listener
      cnObj.on('close', () => {
        utils.logger.info('[connect] - connection closed');
        this.bmqConnectionsPool[cnObj.poolId].connection = null;
        delete this.bmqConnectionsPool[cnObj.poolId];
      });

      cnObj.on('error', (err) => {
        utils.logger.error('[connect] - connection:', err);
      });

      // open amqp channel
      return cnObj.createChannel()
      .then((channel) => {
        const chObj = Object.assign(channel);
        chObj.poolId = poolId;

        utils.logger.info('[connect] - channel openned');

        // set listener
        chObj.on('close', () => {
          utils.logger.info('[connect] - channel closed');
          this.bmqConnectionsPool[chObj.poolId].channel = null;
          this.bmqConnectionsPool[chObj.poolId].connection.close();
        });

        chObj.on('error', (err) => {
          utils.logger.error('[connect] - channel:', err);
        });

        // set prefetch
        chObj.prefetch(this.bmqConfig.prefetch);

        this.bmqConnectionsPool[poolId] = {
          config: this.bmqConfig,
          clientProperties,
          channel: chObj,
          connection: cnObj,
        };

        return chObj;
      });
    })
    .catch((err) => {
      utils.logger.error('[connect]: ', err);
      return utils.promiseWait(this.bmqConfig.timeout)
      .then(() => this.connect(config));
    });
  }

  publish(exchange, routingKey, msg, options) {
    const transactionOption = utils.getPublishOptions(Object.assign(this.bmqConfig, options.config), options);

    return this.connect(transactionOption.config)
    .then((channel) => {
      utils.logger.info(`[publish] - check for exchange: '${exchange}'`);

      return channel.checkExchange(exchange)
      .then(() => {
        channel.assertExchange(exchange, transactionOption.exchange.type, transactionOption.exchange);
        channel.publish(exchange, routingKey, utils.encodeMsg(msg, transactionOption), transactionOption);
        utils.logger.info(`[publish] - message published { exchange: '${exchange}', key: '${routingKey}' } > `, msg);

        return channel;
      });
    })
    .then(channel => utils.promiseWait(this.bmqConfig.timeout).then(() => channel.close()))
    .catch((err) => {
      utils.logger.error('[publish]: ', err);
      return utils.promiseWait(this.bmqConfig.timeout)
      .then(() => this.publish(exchange, routingKey, msg, options));
    });
  }

  subscribe(exchange, queue, routingKey, options, callback) {
    let listener = callback;

    if (typeof options === 'function') {
      listener = options;
    }

    if (typeof callback !== 'function') {
      listener = () => {};
    }

    const transactionOption = utils.getSubscribeOptions(Object.assign(this.bmqConfig, options.config), options);

    return this.connect(transactionOption.config)
    .then((channel) => {
      channel.assertExchange(exchange, transactionOption.exchange.type, transactionOption.exchange);

      return channel.assertQueue(queue, transactionOption.queue)
      .then((q) => {
        channel.bindQueue(q.queue, exchange, routingKey);

        return channel.consume(q.queue, (msg) => {
          const msgContent = utils.decodeMsg(msg, transactionOption);

          utils.logger.info(`[subscribe] - message received
            { exchange: '${exchange}', key: '${routingKey}', queue: '${queue}' } < `.replace(/\s+/g, ' ')
            , msgContent);

          Promise.resolve(msgContent)
          .then(() => listener(msgContent, { options: transactionOption, queue: q.queue, exchange, routingKey }))
          .then(() => {
            if (!transactionOption.noAck) channel.ack(msg);
          })
          .catch((err) => {
            if (!transactionOption.noAck) {
              channel.nack(msg, false, false);
            }

            const opt = Object.assign({}, transactionOption);
            opt.headers = msg.properties.headers;
            opt.contentType = msg.properties.contentType;
            if (opt.headers.requeueLimit > 1 && opt.headers.requeue) {
              opt.headers.requeueLimit -= 1;
              channel.sendToQueue(q.queue, utils.encodeMsg(msgContent, opt), opt);
            }

            utils.logger.error(err);
          });
        }, { noAck: transactionOption.noAck });
      });
    })
    .catch((err) => {
      utils.logger.error('[subscribe]: ', err);
      return utils.promiseWait(this.bmqConfig.timeout)
      .then(() => this.subscribe(exchange, queue, routingKey, options, listener));
    });
  }
}

module.exports = BunnyMq;
