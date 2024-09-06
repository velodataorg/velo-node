const https = require('https')
const { DateTime } = require('luxon')
const ws = require('ws')
const iterator = require('./iterator.js')

const host = 'api.velo.xyz'
const wssPath = 'wss://api.velo.xyz/api/w/connect'

const exchangesF = ['binance-futures', 'bybit', 'deribit', 'okex-swap', 'hyperliquid']
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
    response.on('error', rej)
    response.on('end', () => res(string))
    response.on('data', (chunk) => string += chunk)
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

function alignResolution(params) {
  if (typeof params.resolution === 'number') { return params }
  if (typeof params.resolution !== 'string') { return params }

  let begin = parseInt(params.begin)
  let end = parseInt(params.end)
  if (isNaN(begin) || begin < 0) { throw new Error('invalid begin') }
  if (isNaN(end) || end < 0 || end <= begin) { throw new Error('invalid end') }

  const units = {
    m: 1000 * 60,
    h: 1000 * 60 * 60,
    d: 1000 * 60 * 60 * 24,
    w: 'w',
    M: 'M',
  }

  let reso = params.resolution
  let unit = Object.keys(units).find((key) => reso.endsWith(key))
  if (!unit) { throw new Error('invalid resolution') }
  unit = units[unit]
  reso = parseInt(reso)
  if (isNaN(reso) || reso <= 0) { throw new Error('invalid resolution') }

  if (typeof unit === 'number') {
    const step = reso * unit
    begin = begin - (begin % step)
    if ((end % step) !== 0) { end = step + (end - (end % step)) }
    reso = (reso * unit) / (1000 * 60)
    return { ...params, resolution: reso, begin, end }
  }

  if (unit !== 'w' && unit !== 'M') { throw new Error('invalid resolution') }
  begin = DateTime.fromMillis(begin, {zone: 'UTC'})
  end = DateTime.fromMillis(end, {zone: 'UTC'})

  if (unit === 'w') {
    begin = begin.startOf('week')
    if (end.startOf('week').ts !== end.ts) { end = end.endOf('week').plus({minutes: 1}).startOf('week') }
    reso = 60 * 24 * 7 * reso
  }

  let months = false
  if (unit === 'M') {
    begin = begin.startOf('month')
    if (end.startOf('month').ts !== end.ts) { end = end.endOf('month').plus({minutes: 1}).startOf('month') }
    months = true
  }

  return { ...params, resolution: reso, months, begin: begin.ts, end: end.ts }
}

function paramsToSteps(params) {
  const res = []
  let type = params.type
  let reso = parseInt(params.resolution)
  let begin = parseInt(params.begin)
  let end = parseInt(params.end)
  let exchanges = params.exchanges
  let products = params.products
  let coins = params.coins
  let columns = params.columns
  let months = params.months

  if (type !== 'futures' && type !== 'spot' && type !== 'options') { throw new Error('invalid type') }
  if (isNaN(begin) || begin < 0) { throw new Error('invalid begin') }
  if (isNaN(end) || end < 0 || end <= begin) { throw new Error('invalid end') }
  if (isNaN(reso) || reso <= 0) { throw new Error('invalid resolution') }
  if (columns.indexOf('3m_basis_ann') < 0 && !Array.isArray(exchanges)) { throw new Error('invalid exchanges') }
  if (!Array.isArray(products) && !Array.isArray(coins)) { throw new Error('invalid products or coins') }
  if (!Array.isArray(columns) || columns.length <= 0) { throw new Error('invalid columns') }

  if (columns.indexOf('3m_basis_ann') < 0) {
    if (exchanges.length === 0 && type === 'futures') {
      params.exchanges = exchangesF
    } else if (exchanges.length === 0 && type === 'spot') {
      params.exchanges = exchangesS
    } else if (exchanges.length === 0 && type === 'options') {
      params.exchanges = exchangesO
    }
  }

  if (products) {
    products = products.length
  } else {
    products = coins.length
  }

  if (!Array.isArray(exchanges)) {
    exchanges = 3
  } else {
    exchanges = params.exchanges.length
  }

  columns = columns.length
  let count = Math.ceil((end - begin) / (1000 * 60 * reso)) * exchanges * products * columns
  if (!months && count <= 22500) { return [params] }

  let step = Object.assign({}, params)
  if (step.months) {
    step.end = DateTime.fromMillis(step.begin, {zone: 'UTC'}).plus({months: reso}).ts
  } else {
    count = Math.floor(22500 / exchanges / products / columns)
    step.end = step.begin + ((60 * 1000 * reso) * count)
  }
  res.push(step)

  while (step.end < end) {
    begin = step.end
    step = Object.assign({}, params)
    step.begin = begin
    step.end = step.begin + ((60 * 1000 * reso) * count)
    step.end = Math.min(step.end, parseInt(params.end))
    if (step.months) { step.end = DateTime.fromMillis(step.begin, {zone: 'UTC'}).plus({months: reso}).ts }
    res.push(step)
  }

  return res
}

function csvLinesToObjs(lines) {
  const objs = []
  const first = lines.shift()
  const cols = first.split(',')
  let nums = 3
  if (cols.indexOf('circ_dollars') >= 0) {
    nums = 1
  } else if (cols.indexOf('at_the_money_iv') >= 0) {
    nums = 1
  }

  for (line of lines) {
    const obj = { }
    const vals = line.split(',')
    for (c = 0; c < cols.length; c++) {
      if (c >= nums) { vals[c] = parseFloat(vals[c]) }
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
        if (response.statusCode < 200 || response.statusCode >= 300) {
          rej(new Error(`http status ${response.statusCode}, ${body}`))
        } else if (path.includes('status') || path.includes('/api/n/news')) {
          res(body)
        } else {
          res(csvLinesToObjs(body.split("\n")))
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

const Client = function(key, retry=2) {
  this.key = key
  this.retry = retry
  this.news = {
    stream: this.stream.bind(this),
    stories: this.stories.bind(this)
  }
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
    if (err.message.includes('500') && retry < this.retry) { return this.__products(type, retry + 1) }
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
      if (err.message.includes('500') && retry < this.retry) { return rows(params, retry + 1) }
      throw err
    }
  }

  params = alignResolution(params)
  const steps = paramsToSteps(params)
  return iterator(steps, rows)
}

Client.prototype.marketCaps = async function(params, retry=0) {
  if (retry > 0) { await sleep(2000) }
  const path = '/api/v1/caps'

  try {

    const res = await request(path, params, this.key)
    return res

  } catch (err) {
    if (err.message.includes('429')) { return this.marketCaps(params, retry + 1) }
    if (err.message.includes('500') && retry < this.retry) { return this.marketCaps(params, retry + 1) }
    throw err
  }
}

Client.prototype.termStructure = async function(params, retry=0) {
  if (retry > 0) { await sleep(2000) }
  const path = '/api/v1/terms'

  try {

    const res = await request(path, params, this.key)
    return res

  } catch (err) {
    if (err.message.includes('429')) { return this.termStructure(params, retry + 1) }
    if (err.message.includes('500') && retry < this.retry) { return this.termStructure(params, retry + 1) }
    throw err
  }
}

Client.prototype.stream = function() {
  const auth = Buffer.from(`api:${this.key}`).toString('base64')
  const socket = new ws.WebSocket(wssPath, { headers: {
    'Authorization': `Basic ${auth}`
  }})
  socket.on('open', () => socket.send('subscribe news_priority'))
  return socket
}

Client.prototype.stories = async function(params, retry=0) {
  if (retry > 0) { await sleep(2000) }
  const path = '/api/n/news'

  try {

    const res = await request(path, params, this.key)
    const json = JSON.parse(res)
    return json.stories

  } catch (err) {
    if (err.message.includes('429')) { return this.stories(params, retry + 1) }
    if (err.message.includes('500') && retry < this.retry) { return this.stories(params, retry + 1) }
    throw err
  }
}

Client.prototype.futuresColumns = function() {
  return [
    'open_price',
    'high_price',
    'low_price',
    'close_price',
    'coin_volume',
    'dollar_volume',
    'buy_trades',
    'sell_trades',
    'total_trades',
    'buy_coin_volume',
    'sell_coin_volume',
    'buy_dollar_volume',
    'sell_dollar_volume',
    'coin_open_interest_high',
    'coin_open_interest_low',
    'coin_open_interest_close',
    'dollar_open_interest_high',
    'dollar_open_interest_low',
    'dollar_open_interest_close',
    'funding_rate',
    'premium',
    'buy_liquidations',
    'sell_liquidations',
    'buy_liquidations_coin_volume',
    'sell_liquidations_coin_volume',
    'liquidations_coin_volume',
    'buy_liquidations_dollar_volume',
    'sell_liquidations_dollar_volume',
    'liquidations_dollar_volume',
    '3m_basis_ann'
  ]
}

Client.prototype.optionsColumns = function() {
  return [
    'iv_1w',
    'iv_1m',
    'iv_3m',
    'iv_6m',
    'skew_1w',
    'skew_1m',
    'skew_3m',
    'skew_6m',
    'vega_coins',
    'vega_dollars',
    'call_delta_coins',
    'call_delta_dollars',
    'put_delta_coins',
    'put_delta_dollars',
    'gamma_coins',
    'gamma_dollars',
    'call_volume',
    'call_premium',
    'call_notional',
    'put_volume',
    'put_premium',
    'put_notional',
    'dollar_volume',
    'dvol_open',
    'dvol_high',
    'dvol_low',
    'dvol_close',
    'index_price'
  ]
}

Client.prototype.spotColumns = function() {
  return [
    'open_price',
    'high_price',
    'low_price',
    'close_price',
    'coin_volume',
    'dollar_volume',
    'buy_trades',
    'sell_trades',
    'total_trades',
    'buy_coin_volume',
    'sell_coin_volume',
    'buy_dollar_volume',
    'sell_dollar_volume'
  ]
}

Client.prototype.version = function() {
  return '1.5.2'
}

module.exports = Client
