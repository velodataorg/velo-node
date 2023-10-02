const https = require('https')
const { DateTime } = require('luxon')
const iterator = require('./iterator.js')

const host = 'velodata.app'

const exchangesF = ['binance-futures', 'bybit', 'deribit', 'okex-swap']
const exchangesS = ['binance', 'coinbase']
const exchangesO = ['deribit']

function sleep(ms) {
  return new Promise((res, rej) => {
    setTimeout(res, ms)
  })
}

function readBody(response) {
  return new Promise((res, rej) => {
    let string = ''
    response.setEncoding('utf8')
    response.on('data', (chunk) => {
      string += chunk
    })
    response.on('error', rej)
    response.on('end', () => res(string))
  })
}

function addParamsToPath(path, params) {
  let res = path + '?'
  for (p of Object.keys(params)) {
    res += `${p}=`
    if (Array.isArray(params[p])) {
      res += params[p].join(',') + "&"
    } else {
      res += params[p] + "&"
    }
  }
  return res
}

function paramsToSteps(params) {
  const res = []
  let type = params.type
  let reso = parseInt(params.resolution)
  let begin = parseInt(params.begin)
  let end = parseInt(params.end)
  let exchanges = params.exchanges
  let products = params.products
  let columns = params.columns

  if (type !== 'futures' && type !== 'spot' && type !== 'options') { throw new Error('invalid type') }
  if (isNaN(begin) || begin < 0) { throw new Error('invalid begin') }
  if (isNaN(end) || end < 0 || end <= begin) { throw new Error('invalid end') }
  if (isNaN(reso) || reso <= 0) { throw new Error('invalid resolution') }
  if (!Array.isArray(exchanges)) { throw new Error('invalid exchanges') }
  if (!Array.isArray(products) || products.length <= 0) { throw new Error('invalid products') }
  if (!Array.isArray(columns) || columns.length <= 0) { throw new Error('invalid columns') }

  if (exchanges.length === 0 && type === 'futures') {
    params.exchanges = exchangesF
  } else if (exchanges.length === 0 && type === 'spot') {
    params.exchanges = exchangesS
  } else if (exchanges.length === 0 && type === 'options') {
    params.exchanges = exchangesO
  }

  products = products.length
  columns = columns.length
  exchanges = params.exchanges.length

  let count = Math.ceil((end - begin) / (1000 * 60 * reso)) * exchanges * products * columns
  if (count <= 22500) { return [params] }

  let step = Object.assign({}, params)
  count = 22500 / exchanges / products / columns
  step.end = step.begin + ((60 * 1000 * reso) * count)
  res.push(step)

  while (step.end < end) {
    begin = step.end
    step = Object.assign({}, params)
    step.begin = begin
    step.end = step.begin + ((60 * 1000 * reso) * count)
    step.end = Math.min(step.end, parseInt(params.end))
    res.push(step)
  }

  return res
}

function csvLinesToObjs(lines) {
  const objs = []
  const first = lines.shift()
  const cols = first.split(',')
  for (line of lines) {
    const obj = { }
    const vals = line.split(',')
    for (c = 0; c < cols.length; c++) {
      if (c >= 3) { vals[c] = parseFloat(vals[c]) }
      obj[cols[c]] = vals[c]
    }
    if (line) { objs.push(obj) }
  }
  return objs
}

function request(path, params, key) {
  path = addParamsToPath(path, params)
  const auth = Buffer.from(`api:${key}`).toString('base64')
  return new Promise((res, rej) => {
    const options = {
      host, path, port: 443, method: 'GET',
      headers: { 'Authorization': `Basic ${auth}` }
    }

    const request = https.request(options, (response) => {
      readBody(response).then((body) => {
        if (path.includes('status')) {
          res(body)
        } else if (response.statusCode === 200) {
          res(csvLinesToObjs(body.split("\n")))
        } else {
          rej(new Error(`http status ${response.statusCode}, ${body}`))
        }
      }).catch(rej)
    })

    request.on('error', rej)
    request.end()
  })
}

function requestStreaming(path, params, key) {
  path = addParamsToPath(path, params)
  const auth = Buffer.from(`api:${key}`).toString('base64')
  return new Promise((res, rej) => {
    const options = {
      host, path, port: 443, method: 'GET',
      headers: { 'Authorization': `Basic ${auth}` }
    }

    const request = https.request(options, (response) => {
      if (response.statusCode !== 200) {
        return readBody(response).then((body) => {
          rej(new Error(`http status ${response.statusCode}, ${body}`))
        }).catch(rej)
      }
      response.setEncoding('utf8')
      res(response)
    })

    request.on('error', rej)
    request.end()
  })
}

const Client = function(key) {
  this.key = key
}

Client.prototype.status = async function() {
  const path = '/api/v1/status'
  const res = await request(path, { }, this.key)
  return res
}

Client.prototype.__products = async function(type, retry=0) {
  if (retry > 0) { await sleep(2000) }

  let path = null
  if (type === 1) {
    path = '/api/v1/futures'
  } else if (type === 2) {
    path = '/api/v1/spot'
  } else {
    path = '/api/v1/options'
  }

  try {

    const res = await request(path, { }, this.key)
    return res

  } catch (err) {
    if (err.message.includes('429')) { return this.__products(type, retry + 1) }
    throw err
  }
}

Client.prototype.futures = function() {
  return this.__products(1)
}

Client.prototype.spot = function() {
  return this.__products(2)
}

Client.prototype.options = function() {
  return this.__products(3)
}

Client.prototype.rows = function(params) {
  const key = this.key
  const path = '/api/v1/rows'

  async function rows(params, retry=0) {
    if (retry > 0) { await sleep(2000) }
    try {

      const res = await requestStreaming(path, params, key)
      return res

    } catch (err) {
      if (err.message.includes('429')) { return rows(params, retry + 1) }
      throw err
    }
  }

  const steps = paramsToSteps(params)
  return iterator(steps, rows)
}

module.exports = Client
