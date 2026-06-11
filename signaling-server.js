// WebSocket signaling server for WebRTC (Node.js)
// Local dev:  node signaling-server.js   ->  ws://localhost:3001
// Production (Render/Railway/Fly): they set process.env.PORT and front it with
// HTTPS, so your app uses wss://<your-service-host>/  (no /ws path needed).

const http = require('http');
const WebSocket = require('ws');

const PORT = process.env.PORT || 3001;

// A tiny HTTP server so cloud platforms' health checks get a 200 OK, and the
// WebSocket upgrade rides on the same port (required on Render/Railway).
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('TLC signaling server OK');
});

const wss = new WebSocket.Server({ server });

let rooms = {};
let lockedRooms = {};
let idCounter = 1;

wss.on('connection', function connection(ws) {
  ws.id = idCounter++;
  ws.room = null;

  ws.on('message', function incoming(message) {
    let data;
    try {
      data = JSON.parse(message);
    } catch (e) {
      return;
    }
    if (data.join) {
      ws.room = data.join;
      // Reject new joiners if the host locked the meeting
      if (lockedRooms[ws.room]) {
        ws.send(JSON.stringify({ type: 'meeting_locked' }));
        return;
      }
      if (!rooms[ws.room]) rooms[ws.room] = [];
      rooms[ws.room].push(ws);
      ws.send(JSON.stringify({ joined: true, id: ws.id }));
      // Notify others in the room
      rooms[ws.room].forEach(client => {
        if (client !== ws) client.send(JSON.stringify({ joined: true, id: ws.id }));
      });
    } else if (data.offer || data.answer || data.signal) {
      // Relay offer/answer/signal to the target peer
      let target = null;
      if (ws.room && rooms[ws.room]) {
        target = rooms[ws.room].find(client => client.id === data.to);
      }
      if (target) {
        target.send(JSON.stringify({ ...data, from: ws.id }));
      }
    } else if (data.type) {
      // Room-wide broadcast: chat / raise hand / host controls / lock
      if (data.type === 'lock' && ws.room) {
        lockedRooms[ws.room] = !!data.locked;
      }
      if (ws.room && rooms[ws.room]) {
        const payload = JSON.stringify({ ...data, from: ws.id });
        rooms[ws.room].forEach(client => {
          if (client !== ws && client.readyState === 1) client.send(payload);
        });
      }
    }
  });

  ws.on('close', function() {
    if (ws.room && rooms[ws.room]) {
      rooms[ws.room] = rooms[ws.room].filter(client => client !== ws);
      // Optionally notify others of disconnect
    }
  });
});

server.listen(PORT, () => {
  console.log('WebRTC signaling server running on port ' + PORT);
});
