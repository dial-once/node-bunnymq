
require('dotenv').load();
const utils = require('./modules/utils');

// console.log(process.env);

console.log(utils.setConfig({
  debug: true,
  amqpRequeue: false,
}));
