const uuid = require('uuid');
const zlib = require('zlib');

class Message {
  constructor(msg) {
    this.requestId = msg.requestId || uuid.v4();
    this.messageId = msg.messageId || uuid.v4();
    this.from = msg.from || 'unknown';
    this.to = msg.to || 'unknown';
    this.timestamp = new Date();
    this.priority = msg.priority || 1;
    this.body = msg.body || '';
    this.correlationId = msg.correlationId;
    this.replyTo = msg.replyTo;
    this.ttl = msg.ttl || 0;
    this.type = msg.type || 'msg';
  }

  set setMessageId(value) {
    this.messageId = value;
  }

  get getMessageId() {
    return this.messageId;
  }

  static compress(input) {
    return new Promise((resolve, reject) => {
      zlib.gzip(input, (err, data) => {
        if (!err) return resolve(data);

        if (err.message.includes('incorrect header check')) return resolve(input);

        return reject(err);
      });
    });
  }

  static decompress(input) {
    return new Promise((resolve, reject) => {
      zlib.gunzip(input, (err, data) => {
        if (!err) return resolve(data);

        if (err.message.includes('incorrect header check')) return resolve(input);

        return reject(err);
      });
    });
  }

  static serialize(message, enableCompression) {
    console.log('message:', message);
    return new Promise((resolve, reject) => {
      const msg = Buffer.from(JSON.stringify(message), 'utf-8');

      if (!enableCompression) return resolve(msg);

      return Message.compress(msg).then(resolve).catch(reject);
    });
  }

  static deserialize(message, metadata = {}) {
    return new Promise((resolve, reject) => {
      return Message.decompress(message)
      .then((decompressedMsg) => {
        const msg = Object.assign({}, JSON.parse(decompressedMsg.toString()));
        const deserializedMsg = new Message(msg);

        deserializedMsg.routingKey = metadata.routingKey;
        deserializedMsg.exchange = metadata.exchange;
        deserializedMsg.redelivered = msg.redelivered || metadata.redelivered;
        deserializedMsg.consumerTag = metadata.consumerTag;

        return resolve(deserializedMsg);
      })
      .catch(reject);
    });
  }
}

module.exports = Message;
