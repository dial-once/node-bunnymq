const amqp = require('amqplib');
const ChannelPool = require('./channel-pool');
const packageVersion = require('../package.json').version;
const utils = require('./modules/utils');
const events = require('events');

let instance;

class Connection {
  constructor(config, debug) {
    if (!instance) {
      instance = this;

      this.config = config;
      this.connection = undefined;
      this.state = Connection.STATES.DISCONNECTED;
      this.channelPool = null;
      this.startedAt = new Date().toISOString();
      this.eventEmitter = new events.EventEmitter();
      this.eventEmitter.setMaxListeners(0);
      this.debug = debug;
      // methods binding
      this.connect = this.connect.bind(this);
      this.createChannel = this.createChannel.bind(this);
      this.getChannel = this.getChannel.bind(this);
      this.releaseChannel = this.releaseChannel.bind(this);
    }

    return instance;
  }

  static get STATES() {
    return {
      DISCONNECTED: -1,
      CONNECTING: 0,
      CONNECTED: 1
    };
  }

  connect() {
    return new Promise((resolve, reject) => {
      if (this.state === Connection.STATES.CONNECTED) {
        this.eventEmitter.emit('connected', this.connection);
        return resolve(this.connection);
      }

      if (this.state === Connection.STATES.DISCONNECTED) {
        this.state = Connection.STATES.CONNECTING;

        // set clientProperties to get more info about connection
        const clientProperties = {
          hostname: this.config.hostname,
          bunnymq: packageVersion,
          startedAt: this.startedAt,
          connectedAt: new Date().toISOString()
        };

        // open amqp connection
        amqp.connect(this.config.host, { clientProperties })
        .then((connection) => {
          this.connection = connection;

          const pool = new ChannelPool(this.config.poolSize, this.connection);

          const promisePoolCreation = pool.create()
          .then(() => {
            this.channelPool = pool;
            this.state = Connection.STATES.CONNECTED;
            this.eventEmitter.emit('connected', this.connection);
            return this.connection;
          });

          this.debug('connection openned');

          // set listeners
          this.connection.on('close', () => {
            this.debug('connection closed');
            this.connection = null;
          });

          this.connection.on('error', (err) => {
            this.debug(err);
            return reject(err);
          });

          return resolve(promisePoolCreation);
        })
        .catch(() => {
          this.state = Connection.STATES.DISCONNECTED;
          return resolve(utils.timeoutPromise(this.config.timeout)
          .then(() => this.connect()));
        });
      }
    });
  }

  releaseChannel(channel) {
    return this.channelPool.releaseChannel(channel);
  }

  getChannel(channel) {
    return this.channelPool.getChannel(channel);
  }

  createChannel(resolve) {
    return this.connection.createChannel()
    .then((channel) => {
      channel.on('close', () => {
        this.debug('channel closed');
      });

      channel.on('error', (err) => {
        this.debug('channel:', err);
        throw err;
      });

      return channel;
    });
  }
}

module.exports = Connection;
