const http = require('http');
const { Server, WebSocket } = require('ws');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const HOST = process.env.HOST || '0.0.0.0';
const MAX_PAYLOAD_SIZE = 64 * 1024;
const SERVER_START_TIME = Date.now();

const ALLOWED_ORIGINS = [
  'https://incredible-sunshine-3d7f48.netlify.app'
];

const IS_PRODUCTION = process.env.NODE_ENV === 'production';

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'UP',
      uptimeSeconds: Math.floor((Date.now() - SERVER_START_TIME) / 1000),
      connections: clients.size,
      activeRooms: rooms.size,
      queueSize: waitingQueue.length,
      timestamp: new Date().toISOString()
    }));
    return;
  }
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'Not Found' }));
});

const wss = new Server({
  noServer: true,
  maxPayload: MAX_PAYLOAD_SIZE
});

server.on('upgrade', (request, socket, head) => {
  const origin = request.headers.origin;
  if (IS_PRODUCTION && (!origin || !ALLOWED_ORIGINS.includes(origin))) {
    socket.write('HTTP/1.1 403 Forbidden\r\n\r\n');
    socket.destroy();
    return;
  }
  wss.handleUpgrade(request, socket, head, (ws) => {
    wss.emit('connection', ws, request);
  });
});

const clients = new Map();
const rooms = new Map();
const waitingQueue = [];

wss.on('connection', (ws) => {
  const peerId = 'u_' + crypto.randomBytes(8).toString('hex');
  clients.set(ws, {
    id: peerId,
    name: 'عابر',
    roomId: null,
    msgCount: 0,
    lastReset: Date.now()
  });

  ws.on('message', (raw) => {
    const client = clients.get(ws);
    if (!client) return;
    const now = Date.now();
    if (now - client.lastReset > 1000) {
      client.msgCount = 0;
      client.lastReset = now;
    }
    client.msgCount++;
    if (client.msgCount > 30) {
      ws.send(JSON.stringify({ type: 'ERROR', message: 'تجاوزت الحد المسموح' }));
      return;
    }
    try {
      const data = JSON.parse(raw);
      handleMessage(ws, client, data);
    } catch (e) {
      ws.send(JSON.stringify({ type: 'ERROR', message: 'خطأ في البيانات' }));
    }
  });

  ws.on('close', () => cleanUpClient(ws));
  ws.on('error', () => cleanUpClient(ws));
});

function handleMessage(ws, client, data) {
  switch (data.type) {
    case 'REGISTER':
      client.name = String(data.name || 'عابر').slice(0, 24);
      enqueueClient(ws);
      break;
    case 'SIGNAL':
      if (!client.roomId) return;
      const room = rooms.get(client.roomId);
      if (room) {
        for (const peerWs of room) {
          if (peerWs !== ws && peerWs.readyState === WebSocket.OPEN) {
            peerWs.send(JSON.stringify({ type: 'SIGNAL', payload: data.payload }));
          }
        }
      }
      break;
    case 'LEAVE':
    case 'SKIP':
      cleanUpClient(ws, false);
      if (data.type === 'SKIP') enqueueClient(ws);
      else ws.send(JSON.stringify({ type: 'IDLE' }));
      break;
  }
}

function enqueueClient(ws) {
  cleanQueue();
  const client = clients.get(ws);
  if (!client || client.roomId) return;
  if (waitingQueue.includes(ws)) return;
  if (waitingQueue.length > 0) {
    const partnerWs = waitingQueue.shift();
    const partnerClient = clients.get(partnerWs);
    if (partnerWs && partnerWs.readyState === WebSocket.OPEN && partnerClient && !partnerClient.roomId) {
      const roomId = 'rm_' + crypto.randomBytes(8).toString('hex');
      client.roomId = roomId;
      partnerClient.roomId = roomId;
      rooms.set(roomId, new Set([ws, partnerWs]));
      ws.send(JSON.stringify({ type: 'MATCH', roomId, role: 'offerer', partnerName: partnerClient.name }));
      partnerWs.send(JSON.stringify({ type: 'MATCH', roomId, role: 'answerer', partnerName: client.name }));
      return;
    }
  }
  waitingQueue.push(ws);
  ws.send(JSON.stringify({ type: 'WAITING' }));
}

function cleanUpClient(ws, fullDisconnect = true) {
  const client = clients.get(ws);
  const qIndex = waitingQueue.indexOf(ws);
  if (qIndex !== -1) waitingQueue.splice(qIndex, 1);
  if (client && client.roomId) {
    const roomId = client.roomId;
    const room = rooms.get(roomId);
    if (room) {
      room.delete(ws);
      for (const partnerWs of room) {
        const partnerClient = clients.get(partnerWs);
        if (partnerClient) partnerClient.roomId = null;
        if (partnerWs.readyState === WebSocket.OPEN) {
          partnerWs.send(JSON.stringify({ type: 'PARTNER_LEFT' }));
        }
      }
      if (room.size === 0) rooms.delete(roomId);
    }
    client.roomId = null;
  }
  if (fullDisconnect) clients.delete(ws);
}

function cleanQueue() {
  for (let i = waitingQueue.length - 1; i >= 0; i--) {
    const ws = waitingQueue[i];
    if (ws.readyState !== WebSocket.OPEN) waitingQueue.splice(i, 1);
  }
}

server.listen(PORT, HOST, () => {
  console.log(`[LAFI NEXUS Core] Server active on ${HOST}:${PORT}`);
  console.log(`[LAFI NEXUS Core] Production mode: ${IS_PRODUCTION}`);
});
