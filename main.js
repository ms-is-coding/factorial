const { writeFileSync, createWriteStream, appendFileSync } = require("node:fs");
const { fork } = require("node:child_process");
const { cpus } = require("node:os");
const { memoryUsage } = require("node:process");

const primes = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101];

const numCPUs = cpus().length;
const number = 1e8;
const string = number.toString().split(/(\d{1,3})?(\d{3})+?(\d{3})$/g).filter(Boolean).join(",");

const batchSize = 3e4;
const batchCount = Math.floor(number / batchSize) || 1;
const batches = Array.from({ length: batchCount }).fill(Math.floor(number / batchCount));
const factorCount = number.toString().length * 2;
// const factors = primes.slice(0, factorCount);
const factors = [];

const Types = {
  Exit: 0,
  SendBatch: 1,
  ReplyBatch: 2,
  SendProduct: 3,
  ReplyProduct: 4,
  Memory: 5,
};

function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

function minimizeNumber(x) {
  const M = ~~(x / 1e6);
  x -= M*1e6;
  const k = ~~(x / 1e3);
  x -= k*1e3;
  const u = x;
  return `${M ? ` ${M}M` : ""}${k ? ` ${k}k` : ""}${u ? ` ${u}` : ""}`.trim();
}

for (let i = 0; i < number % batchCount; i++) {
  batches[i]++;
}

const state = {
  filesize: 0,
  batchesDone: 0,
  productsDone: 0,
  writes: 0,
  totalWrites: 0,
  // 0 for off, 1 for batch processor, 2 for product processor
  threadStatus: Array.apply(null, { length: numCPUs }).map(x => ({ type: 0, batches: 0, products: 0, memory: 0 })),
  time: "0s"
};

function* getBatch() {
  let counter = 1;
  let list = [];
  for (let i = 0; i < batches.length; i++) {
    list.push({ min: counter, max: counter += batches[i] - 1, count: i + 1 });
    counter++;
  }
  for (let i = list.length - 1; i >= 0; i--) {
    yield list[i];
  }
}

class Thread {
  constructor(id, parent) {
    this.batch = parent.batch;
    this.id = id;
    this.parent = parent;
    this.closed = false;
    this.resolve = () => { };

    this.thread = fork("./thread.js", { env: { FACTORS: factors.toString() } });

    this.thread.on("message", async ({ type, data }) => {
      if (type == Types.ReplyBatch) {
        state.threadStatus[this.id].batches++;
        state.batchesDone++;
      }
      else if (type == Types.ReplyProduct) {
        state.threadStatus[this.id].products++;
        state.productsDone++;
      }
      else if (type == Types.Memory) {
        state.threadStatus[this.id].memory = data;
        return;
      }
      this.parent.queue.push(data);
      this.resolve();
    });
    this.thread.on("error", err => {
      console.error(err);
      process.exit();
    })
  }

  async run() {
    if (this.parent.queue.length > 1) {
      return await this.multiply(this.parent.queue.shift(), this.parent.queue.shift());
    }
    let data = this.batch.next();
    if (!data.done) {
      return await this.compute(data.value);
    } else if (state.batchesDone - state.productsDone > 2) {
      return await sleep(100);
    } else {
      this.close();
    }
  }

  multiply(a, b) {
    state.threadStatus[this.id].type = 2;
    return new Promise(resolve => {
      this.resolve = resolve;
      this.thread.send({ type: Types.SendProduct, data: [a, b] });
    });
  }
  compute(props) {
    state.threadStatus[this.id].type = 1;
    return new Promise(resolve => {
      this.resolve = resolve;
      this.thread.send({ type: Types.SendBatch, data: props });
    });
  }

  close() {
    state.threadStatus[this.id].type = 0;
    this.closed = true;
    this.thread.send({ type: Types.Exit });
  }
}


// TODO manage threads by evenly distributing load
class Cluster {
  constructor(CPUs) {
    this.batch = getBatch();
    this.threads = Array.apply(null, { length: CPUs }).map((_, i) => new Thread(i, this));
    this.queue = [];
  }
  run() {
    return new Promise(resolve => {
      let threadCount = this.threads.length;
      this.threads.forEach(async thread => {
        while (!thread.closed) {
          await thread.run();
        }
        threadCount--;
        if (threadCount == 0) resolve();
      });
    });
  }
  write() {
    let data = this.queue.shift();
    state.filesize = data.value.length;
    let factorization = "";
    for (const factor of factors) {
      factorization += ` * ${factor}e${data.fact[factor]}`;
    }
    let value = data.value.match(/.{1000}/g);
    state.totalWrites = value.length;
    writeFileSync(`fact${minimizeNumber(number)}.txt`, value[0]); // write to create file
    for (let i = 1; i < value.length; i++) {
      appendFileSync(`fact${minimizeNumber(number)}.txt`, value[i]);
      state.writes++;
    }
    appendFileSync(`fact${minimizeNumber(number)}.txt`, factorization + "\n");
    state.writes++;
  }
}

function format(num) {
  return `${" ".repeat(10 - num.toString().length) + num}`;
}

async function render() {
  process.stdout.write("\x1b[1;1H\x1b[2J\x1b[3J");
  console.log(`Running with ${(memoryUsage().heapTotal / 1024**2).toFixed(1)}MiB RAM`);
  console.log();
  console.log(`Computing the factorial of ${string}`);
  console.log(`Total batch count: ${batchCount}`);
  let done = true;
  for (const i in state.threadStatus) {
    let thread = state.threadStatus[i];
    thread.type != 0 ? done = false : 0;
    console.log(`\x1b[${thread.type == 0 ? "30" : thread.type == 1 ? "96" : "95"}mThread ${" ".repeat(2 - (+i + 1).toString().length) + (+i + 1)}\x1b[0m : \x1b[92m${format(thread.batches)} batches \x1b[94m${format(thread.products)} products\x1b[0m${format((thread.memory / 1024**2).toFixed(2))} MiB RAM`);
  }
  if (done) {
    console.log(`Writing... ${(100 * state.writes / state.totalWrites).toFixed(2)}%   (${(state.filesize / 1024 ** 2).toFixed(2)} MiB)`);
  } else {
    console.log(`${~~(100 * (state.productsDone + state.batchesDone) / (batchCount * 2 - 1))}%`);
  }
  console.log(`Elapsed : ${state.time}`);
}

async function main() {
  const start = Date.now();

  setInterval(() => {
    let time = Date.now() - start;
    let hours = (time - (time %= 3600000)) / 3600000;
    let minutes = (time - (time %= 60000)) / 60000;
    let seconds = (time - (time %= 1000)) / 1000;
    state.time = `${hours}h ${minutes}min ${seconds}sec`;
    render();
  }, 100);

  const cluster = new Cluster(numCPUs);
  await cluster.run();

  cluster.write();
  render();
  process.exit();
}

main();
