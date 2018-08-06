/*eslint-disable*/

class Mutex {
  constructor() {
    this.queue = [];
    this.busy = false;

    this.synchronize = this.synchronize.bind(this);
    this.dequeue = this.dequeue.bind(this);
    this.execute = this.execute.bind(this);
  }

  synchronize(task) {
    return new Promise((resolve, reject) => {
      this.queue.push([task, resolve, reject]);
      if (!this.busy) this.dequeue();
    });
  }

  dequeue() {
    this.busy = true;
    const next = this.queue.shift();

    if (next) return this.execute(next);
    this.busy = false;
  }

  execute(record) {
    const task = record[0];
    const resolve = record[1];
    const reject = record[2];

    return task()
    .then(resolve)
    .then(() => {
      this.dequeue();
    })
    .catch(reject);
  }
}

module.exports = Mutex;
