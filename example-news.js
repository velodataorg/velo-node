const velo = require('velo-node')

async function doWork() {
  const stories = await client.news.stories({begin: 0})
  console.log(stories)

  // is instance of https://github.com/websockets/ws/blob/HEAD/doc/ws.md#class-websocket
  const socket = client.news.stream()

  socket.on('open', () => {
    console.log('connected')
  })

  socket.on('message', (data) => {
    const json = JSON.parse(data)
    console.log('received', json)
  })

  socket.on('close', () => {
    console.log('disconnected')
  })

  socket.on('error', console.error)
}

const client = new velo.Client('api_key')

doWork()
