# velo-node
NodeJS library for Velo API access. [See full documentation here](https://velodata.gitbook.io/velo-data-api/nodejs).

## Install
```
npm install velo-node
```

## Usage
```javascript
const velo = require('velo-node')

async function doWork() {
  const futures = await client.futures()
  const future = futures[0]
  
  const columns = await client.futures_columns()
  const twoColumns = columns.slice(0, 2)

  const params = {
    type: 'futures',
    columns: twoColumns,
    exchanges: [future.exchange],
    products: [future.product],
    begin: Date.now() - 1000 * 60 * 11,
    end: Date.now(),
    resolution: 1
  }

  const rows = client.rows(params)
  for await (const row of rows) {
    console.log(row)
  }
}

const client = new velo.Client('api_key')

doWork()
```

## License
Copyright 2023 Velo Data, license MIT
