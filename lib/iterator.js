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

function lineToObj(cols, line) {
  const obj = { }
  const vals = line.split(',')
  for (c = 0; c < cols.length; c++) {
    if (c >= 3) { vals[c] = parseFloat(vals[c]) }
    obj[cols[c]] = vals[c]
  }
  return obj
}

module.exports = function (steps, getNext) {
  const buffer = []
  let buffering = null
  let error = null
  let cols = null

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
            value = lineToObj(cols, value)
            return { done: false, value }
          }

          const params = steps.shift()
          if (!params) { return { done: true } }

          const next = await getNext(params)
          const stream = next.pipe(split())
          stream.on('error', (err) => { error = err })

          buffering = readAllLines(stream).then(() => {
            buffering = null
          })
          await readTwoLines(stream, buffer)

          value = buffer.shift()
          if (!value) { return { done: true } }

          cols = value.split(',')

          value = buffer.shift()
          if (!value) { return { done: true } }

          value = lineToObj(cols, value)
          return { done: false, value }
        }
      }
    }
  }
}
