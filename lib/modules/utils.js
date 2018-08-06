const serializeError = require('serialize-error');
const deserializeError = require('deserialize-error');

function timeoutPromise(timer) {
  return new Promise((resolve) => {
    setTimeout(resolve, timer);
  });
}

function deserializeMsg(msg) {
  // if sender put a json header, we parse it to avoid the pain for the consumer
  if (!msg.content) return undefined;

  if (msg.properties.contentType === 'application/json') {
    try {
      const content = JSON.parse(msg.content.toString());
      if (content && content.error && content.error instanceof Object) {
        content.error = deserializeError(content.error);
      }

      return content;
    } catch (err) {
      console.warn('message is not a valid json');
      return msg.content;
    }
  }

  if (msg.content.length) {
    return msg.content.toString();
  }

  return msg.content;
}

/* eslint-disable no-param-reassign */
function serializeMsg(msg, options) {
  const falsie = [undefined, null];

  if (typeof msg === 'string') return Buffer.from(msg, 'utf-8');

  if (!falsie.includes(msg)) {
    if (msg.error instanceof Error) {
      msg.error = serializeError(msg.error);
    }

    try {
      // if content is not a string, we JSONify it (JSON.parse can handle numbers, etc. so we can skip all the checks)
      msg = JSON.stringify(msg);
      options.contentType = 'application/json';

      return Buffer.from(msg, 'utf-8');
    } catch (err) {
      return Buffer.from([]);
    }
  }

  return Buffer.from([]);
}

function completeAssign(target, ...sources) {
  sources.forEach(source => {
    let descriptors = Object.keys(source).reduce((descriptors, key) => {
      descriptors[key] = Object.getOwnPropertyDescriptor(source, key);
      return descriptors;
    }, {});
    // Par défaut, Object.assign copie également
    // les symboles énumérables
    Object.getOwnPropertySymbols(source).forEach(sym => {
      let descriptor = Object.getOwnPropertyDescriptor(source, sym);
      if (descriptor.enumerable) {
        descriptors[sym] = descriptor;
      }
    });
    Object.defineProperties(target, descriptors);
  });
  return target;
}


class Deferred {
  constructor() {
    this.promise = new Promise((resolve, reject) => {
      this.reject = reject;
      this.resolve = resolve;
    });
  }
}

module.exports = { timeoutPromise, serializeMsg, deserializeMsg, completeAssign, Deferred };
