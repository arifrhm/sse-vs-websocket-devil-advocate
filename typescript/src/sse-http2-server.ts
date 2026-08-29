import http2 from 'http2';

const clients = new Set<http2.ServerHttp2Stream>();

const server = http2.createServer();

server.on('sessionError', (err) => {
  // Suppress session errors (e.g. protocol error, remote connection closed)
});

server.on('error', (err) => {
  console.error('Server error:', err.message);
});

server.on('stream', (stream, headers) => {
  stream.on('error', (err) => {
    // Suppress stream errors (e.g. client aborted stream)
  });

  if (stream.session) {
    stream.session.on('error', (err) => {
      // Suppress session errors on the stream's session
    });
  }

  const path = headers[':path'];
  const method = headers[':method'];

  if (path === '/events') {
    stream.respond({
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache',
      ':status': 200,
    });

    stream.write(': ok\n\n');
    clients.add(stream);

    const keepAliveInterval = setInterval(() => {
      if (!stream.destroyed) {
        stream.write(': keepalive\n\n');
      }
    }, 15000);

    stream.on('close', () => {
      clearInterval(keepAliveInterval);
      clients.delete(stream);
    });
    return;
  }

  if (path === '/post' && method === 'POST') {
    let body = '';
    stream.on('data', chunk => {
      body += chunk.toString();
    });
    stream.on('end', () => {
      try {
        const data = JSON.parse(body);
        stream.respond({
          'content-type': 'application/json',
          ':status': 200,
        });
        stream.end(JSON.stringify({ status: 'ok', echoed: data }));
      } catch (err) {
        stream.respond({ ':status': 400 });
        stream.end('Invalid JSON');
      }
    });
    return;
  }

  if (path === '/health') {
    stream.respond({
      'content-type': 'application/json',
      ':status': 200,
    });
    stream.end(JSON.stringify({ status: 'ok', memory: process.memoryUsage(), clients: clients.size }));
    return;
  }

  stream.respond({ ':status': 404 });
  stream.end();
});

// Broadcast periodic server-to-client updates
setInterval(() => {
  const data = JSON.stringify({ type: 'price', value: (Math.random() * 100).toFixed(2), timestamp: Date.now() });
  for (const client of clients) {
    if (!client.destroyed) {
      client.write(`data: ${data}\n\n`);
    }
  }
}, 1000);

server.listen({ port: 8080, backlog: 65535 }, () => {
  console.log('TypeScript SSE HTTP/2 server running on port 8080 (backlog: 65535)');
});

// Memory reporting
setInterval(() => {
  const mem = process.memoryUsage();
  console.log(`[MEM] RSS: ${(mem.rss / 1024 / 1024).toFixed(2)} MB, Heap: ${(mem.heapUsed / 1024 / 1024).toFixed(2)} MB, Clients: ${clients.size}`);
}, 5000);
