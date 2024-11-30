const { memoryUsage } = require("node:process");

let prod = 1n;
let exp = 0n;

const factors = process.env.FACTORS.split(",").filter(Boolean).map(x => BigInt(x));

function initFactors() {
  return Object.fromEntries(factors.map(factor => [factor, 0]));
}

const Types = {
  Exit: 0,
  SendBatch: 1,
  ReplyBatch: 2,
  SendProduct: 3,
  ReplyProduct: 4,
  Memory: 5,
};

function factorial(min, max) {
  let prod = 1n;
  const fact = initFactors();
  const middle = (min + max) / 2n;

  for (let i = min, j = max; i <= middle; i++, j--) {
    if (i == j) {
      prod *= i;
    }
    // for (const factor of factors) {
    //   while (x % factor == 0n) {
    //     x /= factor;
    //     fact[factor]++;
    //   }
    // }
    prod *= (i * j);
  }

  return {
    prod,
    fact
  };
}

process.on("message", ({ type, data }) => {
  switch (type) {
    case Types.Exit: // exit
      process.exit();
    case Types.SendBatch: // compute batch
      const {
        prod,
        fact: fact_
      } = factorial(BigInt(data.min), BigInt(data.max));
      process.send?.({
        type: Types.ReplyBatch, data: {
          value: prod.toString(),
          fact: fact_
        }
      });
      break;
    case Types.SendProduct:
      let fact = initFactors();
      for (const factor of factors) {
        fact[factor] += data[0].fact[factor] + data[1].fact[factor];
      }
      process.send?.({
        type: Types.ReplyProduct, data: {
          value: (BigInt(data[0].value) * BigInt(data[1].value)).toString(),
          fact
        }
      });
      break;
  }
});

setInterval(() => {
  process.send?.({
    type: Types.Memory, data: memoryUsage().heapUsed
  });
}, 100);
