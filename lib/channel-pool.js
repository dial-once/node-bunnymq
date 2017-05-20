const utils = require('./modules/utils');
const Mutex = require('./modules/mutex');

let instance;
const mutex = new Mutex();

const MAX_POOL_SIZE = 20000;

class channelPool {
  constructor(poolSize, connection) {
    if (!instance) {
      instance = this;

      this.connection = connection;
      this.poolSize = poolSize;
      this.pool = [];
      this.channelCount = 0;
      // methods binding
      this.create = this.create.bind(this);
      this.addChannel = this.addChannel.bind(this);
      this.getChannel = this.getChannel.bind(this);
      this.releaseChannel = this.releaseChannel.bind(this);
    }

    return instance;
  }

  create() {
    return Promise.all(
      Array.from({ length: this.poolSize }).map(() => {
        return this.addChannel();
      })
    );
  }

  addChannel() {
    return this.connection.createChannel()
    .then((channel) => {
      ++this.channelCount;
      this.pool.push(channel);

      return channel;
    });
  }

  releaseChannel(channel) {
    if (this.pool.length < this.poolSize) {
      this.pool.push(channel);
    }
  }

  getChannel() {
    return mutex.synchronize(() => {
      if (this.pool.length) return Promise.resolve(this.pool.shift());

      if (this.channelCount < MAX_POOL_SIZE) return this.addChannel(false);

      return utils.timeoutPromise(Math.floor(Math.random() * 10) + 1)
      .then(this.getChannel);
    });
  }
}

module.exports = channelPool;
