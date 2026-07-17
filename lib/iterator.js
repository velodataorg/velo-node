const split = require('split')

function sleep(ms) {
  return new Promise((res, rej) => {
    setTimeout(res, ms)
  })
}

function readAllLines(stream) {
  return new Promise((res, rej) => {
    stream.on('end', res)
  })
}

function readTwoLines(stream, buffer) {
  let count = 0
  return new Promise((res, rej) => {
    stream.on('end', res)
    stream.on('data', (line) => {
      buffer.push(line)
      count++
      if (count === 2) { res() }
    })
  })
}

function lineToObjDepth(line) {
  const obj = {}
  let vals = line.split(',')
  obj['time'] = parseInt(vals[0])
  obj['mid'] = parseFloat(vals[1])
  vals = vals.slice(2)
  for (let i = 0; i < vals.length; i += 2) {
    const price = vals[i]
    const depth = parseFloat(vals[i+1])
    obj[price] = depth
  }
  return obj
}

function lineToObj(cols, line, depth=false) {
  if (depth) { return lineToObjDepth(line) }
  const obj = { }
  const vals = line.split(',')
  for (let c = 0; c < cols.length; c++) {
    if (c >= 3) { vals[c] = parseFloat(vals[c]) }
    obj[cols[c]] = vals[c]
  }
  return obj
}

module.exports = function (steps, getNext, depth=false) {
  const buffer = []
  let buffering = null
  let error = null
  let cols = null

  async function nextStep() {
    const params = steps.shift()
    if (!params) { return true }

    const next = await getNext(params)
    const stream = next.pipe(split())
    stream.on('error', (err) => { error = err })

    buffering = readAllLines(stream).then(() => {
      buffering = null
    })
    await readTwoLines(stream, buffer)
  }

  return {
    [Symbol.asyncIterator]() {
      return {
        async next() {
          if (error) { throw error }

          let value = buffer.shift()
          while (buffering && !value) {
            await sleep(50)
            value = buffer.shift()
          }

          if (value) {
            value = lineToObj(cols, value, depth)
            return { done: false, value }
          }

          while (true) {
            const done = await nextStep()
            if (done) { return { done: true } }

            value = buffer.shift()
            if (!value) { continue }
            cols = value.split(',')

            value = buffer.shift()
            if (!value) { continue }

            value = lineToObj(cols, value, depth)
            return { done: false, value }
          }

          return { done: true }
        }
      }
    }
  }
}
