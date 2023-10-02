# velo-node
Node.js library for Velo API access see full documentation [here](https://velodata.gitbook.io/velo-data-api/nodejs).

## Install
```
npm install velo-node
```

## Usage
```javascript
const velo = require('velo-node')

async function doSomethingWith(row) {
  // todo: your logic
  console.log(row.exchange, row.coin, row.product, row.time, row.open_price, row.close_price)
}

async function getAllFutures() {
  const allFutures = await client.futures()
  /*
  const allSpot = await client.spot()
  const allOptions = await client.options()
  */
  return allFutures
}

async function getRandomFuture() {
  const allFutures = await getAllFutures()
  let random = Math.floor(Math.random() * allFutures.length)
  random = allFutures[random]
  console.log('random future', random.exchange, random.coin, random.product, random.begin)
  return random
}

async function doWork() {
  const random = await getRandomFuture()
  const params = {
    type: 'futures', // futures, spot, or options
    columns: ['open_price', 'close_price'],
    exchanges: [random.exchange],
    products: [random.product],
    begin: Date.now() - (1000 * 60 * 11), // 10 minutes
    end: Date.now(),
    resolution: 1 // 1 minute
  }

  const rows = client.rows(params)
  for await (const row of rows) {
    await doSomethingWith(row)
  }
}

const client = new velo.Client('your_api_key')

doWork().catch(console.error)
```

## License
Copyright 2023 Velo Data, license MIT
