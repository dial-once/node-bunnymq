var consumer = require('../../index')().consumer;

consumer.connect()
.then(function (_channel) {
  consumer.consume('queueName', function (_msg) {
    throw Error('Any kind of error');
    // return new Promise(function (resolve, reject) {
    //   setTimeout(function () {
    //     resolve(true);
    //   }, 5000);
    // });
  });
});
