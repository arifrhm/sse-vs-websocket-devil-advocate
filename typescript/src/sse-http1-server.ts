import { createServer, IncomingMessage, ServerResponse } from 'http';
import { parse } from 'url';

const clients = new Set<ServerResponse>();

const server = createServer((req, res) => {
  const parsedUrl = parse(req.url || '', true);

  if (parsedUrl.pathname === '/events') {
    // SSE headers
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'Access-Control-Allow-Origin': '*',
    });

    // Write initial comment to establish stream
    res.write(': ok\n\n');
    clients.add(res);

    // Keepalive comment interval to prevent timeouts
    const keepAliveInterval = setInterval(() => {
      res.write(': keepalive\n\n');
    }, 15000);

    req.on('close', () => {
      clearInterval(keepAliveInterval);
      clients.delete(res);
    });
    return;
  }

  if (parsedUrl.pathname === '/post' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => {
      body += chunk.toString();
    });
    req.on('end', () => {
      // Simulate handling bidirectional message
      try {
        const data = JSON.parse(body);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'ok', echoed: data }));
      } catch (err) {
        res.writeHead(400);
        res.end('Invalid JSON');
      }
    });
    return;
  }

  if (parsedUrl.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', memory: process.memoryUsage(), clients: clients.size }));
    return;
  }

  res.writeHead(404);
  res.end();
});

// Broadcast periodic server-to-client updates
setInterval(() => {
  const data = JSON.stringify({ type: 'price', value: (Math.random() * 100).toFixed(2), timestamp: Date.now() });
  for (const client of clients) {
    client.write(`data: ${data}\n\n`);
  }
}, 1000);

server.listen({ port: 8080, backlog: 65535 }, () => {
  console.log('TypeScript SSE HTTP/1.1 server running on port 8080 (backlog: 65535)');
});

// Memory reporting
setInterval(() => {
  const mem = process.memoryUsage();
  console.log(`[MEM] RSS: ${(mem.rss / 1024 / 1024).toFixed(2)} MB, Heap: ${(mem.heapUsed / 1024 / 1024).toFixed(2)} MB, Clients: ${clients.size}`);
}, 5000);
