import { WebSocketServer, WebSocket } from 'ws';
import { createServer } from 'http';

const server = createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', memory: process.memoryUsage() }));
    return;
  }
  res.writeHead(404);
  res.end();
});

const wss = new WebSocketServer({ noServer: true, perMessageDeflate: false });

interface ExtWebSocket extends WebSocket {
  isAlive?: boolean;
}

wss.on('connection', (ws: ExtWebSocket) => {
  ws.isAlive = true;

  ws.on('error', console.error);

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  // Server-to-client updates (e.g. price)
  const interval = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'price', value: (Math.random() * 100).toFixed(2), timestamp: Date.now() }));
    }
  }, 1000);

  // Handle client-to-server bidirectional messages
  ws.on('message', (message) => {
    // Just a quick parsing / processing to simulate load
    try {
      const data = JSON.parse(message.toString());
      if (data.type === 'ping') {
        ws.send(JSON.stringify({ type: 'pong' }));
      }
    } catch (e) {
      // Ignore parse errors
    }
  });

  ws.on('close', () => {
    clearInterval(interval);
  });
});

// Ping/Pong connection clean-up interval
const heartbeatInterval = setInterval(() => {
  wss.clients.forEach((ws: ExtWebSocket) => {
    if (ws.isAlive === false) {
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.ping();
  });
}, 10000);

server.on('upgrade', (request, socket, head) => {
  wss.handleUpgrade(request, socket, head, (ws) => {
    wss.emit('connection', ws, request);
  });
});

server.listen({ port: 8080, backlog: 65535 }, () => {
  console.log('TypeScript WebSocket server running on port 8080 (backlog: 65535)');
});

// Memory reporting
setInterval(() => {
  const mem = process.memoryUsage();
  console.log(`[MEM] RSS: ${(mem.rss / 1024 / 1024).toFixed(2)} MB, Heap: ${(mem.heapUsed / 1024 / 1024).toFixed(2)} MB, Clients: ${wss.clients.size}`);
}, 5000);
