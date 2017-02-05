
const uuid = require('uuid');
const crypto = require('crypto');

const algorithm = 'aes-256-ctr';
let transport = console;
let debug = false;

function wrapLogger(severity, msg, metadata) {
  if (!debug) return '';

  if (typeof msg !== 'string') {
    return transport[severity](`[BunnyMq][${new Date().toISOString()}] - `, msg || '');
  }

  return transport[severity](`[BunnyMq][${new Date().toISOString()}]${msg}`, metadata || '');
}

const logger = {
  info: (msg, metadata) => wrapLogger('info', msg, metadata),
  log: (msg, metadata) => wrapLogger('log', msg, metadata),
  warn: (msg, metadata) => wrapLogger('warn', msg, metadata),
  error: (msg, metadata) => wrapLogger('error', msg, metadata),
};

function encrypt(msg, privateKey) {
  if (!privateKey) return new Buffer(msg);

  const cipher = crypto.createCipher(algorithm, privateKey);
  return Buffer.concat([cipher.update(msg), cipher.final()]);
}

function decrypt(msg, privateKey) {
  if (!privateKey) return msg.toString();

  const decipher = crypto.createDecipher(algorithm, privateKey);
  return Buffer.concat([decipher.update(msg), decipher.final()]).toString();
}

function encodeMsg(msg, options) {
  if (msg === undefined) return new Buffer(0);

  let newMsg = Object.assign({}, msg);

  // if content is not a string, we JSONify it (JSON.parse can handle numbers, etc. so we can skip all the checks)
  if (typeof newMsg !== 'string') {
    newMsg = JSON.stringify(newMsg);
    options.contentType = 'application/json'; // eslint-disable-line no-param-reassign
  }

  return encrypt(newMsg, options.config.privateKey);
}

function getSubscribeOptions(config, options) {
  const finalOptions = Object.assign({ noAck: false }, options);

  finalOptions.config = Object.assign({}, config);
  finalOptions.queue = Object.assign({ exclusive: true }, finalOptions.queue);
  finalOptions.exchange = Object.assign({ type: 'direct', durable: true }, finalOptions.exchange);
  finalOptions.headers = Object.assign({
    requeue: finalOptions.config.requeue,
    requeueLimit: finalOptions.config.requeueLimit
  }, finalOptions.headers);

  return finalOptions;
}

function getPublishOptions(config, options) {
  const finalOptions = Object.assign({}, options);

  finalOptions.config = Object.assign({}, config);
  finalOptions.exchange = Object.assign({ type: 'direct', durable: true }, finalOptions.exchange);
  finalOptions.headers = Object.assign({
    requeue: finalOptions.config.requeue,
    requeueLimit: finalOptions.config.requeueLimit
  }, finalOptions.headers);

  return finalOptions;
}

function decodeMsg(msg, options) {
  // if sender put a json header, we parse it to avoid the pain for the consumer
  let newMsg = Object.assign({}, msg);

  newMsg = newMsg.content.length ? decrypt(newMsg.content, options.config.privateKey) : '';

  if (msg.properties.contentType === 'application/json') {
    try {
      return JSON.parse(newMsg);
    } catch (err) {
      logger.info(`[BunnyMq][${new Date()}][decrypt] - Error while decrypting msg, please check your config,
        message will be returned without decryption`);
      return JSON.parse(msg.content.toString());
    }
  }

  return newMsg;
}

function promiseWait(timeout) {
  return new Promise(resolve => setTimeout(resolve, timeout));
}

function setConfig(config) {
  transport = config.logger || config.transport || config.amqpTransport || config.amqpLogger || transport;

  const mergedConfig = [
    {
      host: config.amqpUrl,
      hostname: config.amqpHostname,
      prefetch: config.amqpPrefetch,
      requeue: config.amqpRequeue,
      requeueLimit: config.amqpRequeueLimit,
      timeout: config.amqpTimeout,
      privateKey: config.privateKey,
      debug: config.amqpDebug
    },
    {
      host: process.env.BMQ_URL,
      hostname: process.env.BMQ_HOSTNAME,
      prefetch: process.env.BMQ_PREFETCH,
      requeue: process.env.BMQ_REQUEUE,
      requeueCount: process.env.BMQ_REQUEUE_LIMIT,
      timeout: process.env.BMQ_TIMEOUT,
      privateKey: process.env.BMQ_PRIVATE_KEY,
      debug: process.env.BMQ_DEBUG,
    },
    {
      host: process.env.AMQP_URL,
      hostname: process.env.AMQP_HOSTNAME,
      prefetch: process.env.BMQ_PREFETCH,
      requeue: process.env.AMQP_REQUEUE,
      requeueCount: process.env.AMQP_REQUEUE_LIMIT,
      timeout: process.env.AMQP_TIMEOUT,
      privateKey: process.env.AMQP_PRIVATE_KEY,
      debug: process.env.AMQP_DEBUG,
    },
    {
      host: 'amqp://localhost',
      hostname: process.env.HOSTNAME || uuid.v4(),
      prefetch: 1,
      requeue: true,
      requeueLimit: 3,
      timeout: 1000,
      privateKey: undefined,
      debug: false
    }
  ]
  .reduce((prev, curr) => {
    const next = {};
    ['host', 'hostname', 'prefetch', 'requeue', 'requeueLimit', 'timeout', 'debug', 'privateKey'].forEach((key) => {
      next[key] = prev[key] !== undefined ? prev[key] : curr[key];
    });

    return next;
  }, config);

  debug = mergedConfig.debug || debug;

  return mergedConfig;
}

module.exports = {
  encodeMsg,
  decodeMsg,
  encrypt,
  decrypt,
  promiseWait,
  setConfig,
  getSubscribeOptions,
  getPublishOptions,
  logger,
};
