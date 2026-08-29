import cluster from 'cluster';
import os from 'os';
import { execSync } from 'child_process';
import http from 'http';
import http2 from 'http2';
import WebSocket from 'ws';
import { argv } from 'process';

// CLI parsing
const args = {};
argv.slice(2).forEach(val => {
  const [key, value] = val.split('=');
  const cleanKey = key.replace(/^--/, '');
  args[cleanKey] = value === 'true' ? true : value === 'false' ? false : isNaN(Number(value)) ? value : Number(value);
});

const PROTOCOL = args.protocol || 'ws'; // ws, sse-http1, sse-http2
const CONNECTIONS = args.connections || 100000;
const PORT = args.port || 8080;
const HOST = args.host || 'localhost';
const BIDIRECTIONAL = args.bidirectional || false;
const WRITE_INTERVAL = args.writeInterval || 5000;
const DURATION = args.duration || 30;

// Pool of local IP aliases for 100K connection distribution
const LOCAL_IPS = [
  '127.0.0.1', '127.0.0.2', '127.0.0.3', '127.0.0.4',
  '127.0.0.5', '127.0.0.6', '127.0.0.7', '127.0.0.8'
];

const NUM_WORKERS = Math.min(os.cpus().length || 4, 10);

if (cluster.isPrimary || cluster.isMaster) {
  console.log(`=== 100K Clustered Load Tester (Master PID: ${process.pid}) ===`);
  console.log(`- Protocol: ${PROTOCOL.toUpperCase()}`);
  console.log(`- Total Target Connections: ${CONNECTIONS}`);
  console.log(`- Worker Processes: ${NUM_WORKERS}`);
  console.log(`- Target Connections per Worker: ${Math.ceil(CONNECTIONS / NUM_WORKERS)}`);
  console.log(`- Local IP Pool: ${LOCAL_IPS.join(', ')}`);
  console.log(`- Bidirectional Mode: ${BIDIRECTIONAL}`);
  console.log(`- Duration: ${DURATION}s`);

  let serverPid = null;
  try {
    const lsofOut = execSync(`lsof -t -i :${PORT}`).toString().trim();
    const pids = lsofOut.split('\n').map(p => parseInt(p)).filter(p => !isNaN(p));
    if (pids.length > 0) {
      serverPid = pids[0];
      console.log(`- Detected Target Server PID: ${serverPid}`);
    }
  } catch (e) {
    console.log(`- Warning: Could not detect server PID on port ${PORT}. Memory tracking disabled.`);
  }

  const workerStats = new Map();
  const rssSamples = [];

  const memInterval = setInterval(() => {
    if (!serverPid) return;
    try {
      const rssKb = parseInt(execSync(`ps -o rss= -p ${serverPid}`).toString().trim());
      if (!isNaN(rssKb)) {
        const rssMb = rssKb / 1024;
        rssSamples.push(rssMb);
        let totalConnected = 0;
        for (const stat of workerStats.values()) {
          totalConnected += stat.connected || 0;
        }
        console.log(`[SYS-MEM] Server RSS Memory: ${rssMb.toFixed(2)} MB | Total Connected Clients: ${totalConnected} / ${CONNECTIONS}`);
      }
    } catch (e) {}
  }, 5000);

  // Spawn workers
  const connectionsPerWorker = Math.ceil(CONNECTIONS / NUM_WORKERS);
  for (let i = 0; i < NUM_WORKERS; i++) {
    const worker = cluster.fork({
      WORKER_ID: i,
      WORKER_CONNECTIONS: i === NUM_WORKERS - 1 ? CONNECTIONS - (connectionsPerWorker * i) : connectionsPerWorker,
      PROTOCOL,
      PORT,
      HOST,
      BIDIRECTIONAL,
      WRITE_INTERVAL,
    });

    workerStats.set(worker.id, { connected: 0, errors: 0, received: 0, sent: 0, postLatencySum: 0, postLatencyCount: 0 });

    worker.on('message', (msg) => {
      if (msg.type === 'stats') {
        workerStats.set(worker.id, msg.data);
      }
    });
  }

  // End test after duration
  setTimeout(() => {
    clearInterval(memInterval);
    console.log(`\n--- Test Complete (Aggregating Worker Results) ---`);

    let totalConnected = 0;
    let totalErrors = 0;
    let totalReceived = 0;
    let totalSent = 0;
    let totalPostLatencySum = 0;
    let totalPostLatencyCount = 0;

    for (const stat of workerStats.values()) {
      totalConnected += stat.connected || 0;
      totalErrors += stat.errors || 0;
      totalReceived += stat.received || 0;
      totalSent += stat.sent || 0;
      totalPostLatencySum += stat.postLatencySum || 0;
      totalPostLatencyCount += stat.postLatencyCount || 0;
    }

    const avgRss = rssSamples.length > 0 ? (rssSamples.reduce((a, b) => a + b, 0) / rssSamples.length).toFixed(2) : 'N/A';
    const maxRss = rssSamples.length > 0 ? Math.max(...rssSamples).toFixed(2) : 'N/A';
    const minRss = rssSamples.length > 0 ? Math.min(...rssSamples).toFixed(2) : 'N/A';
    const avgPostLatency = totalPostLatencyCount > 0 ? (totalPostLatencySum / totalPostLatencyCount).toFixed(2) : 'N/A';

    console.log(`Protocol: ${PROTOCOL.toUpperCase()}`);
    console.log(`Peak Connections Achieved: ${totalConnected} / ${CONNECTIONS}`);
    console.log(`Total Errors: ${totalErrors}`);
    console.log(`Messages Received (Server -> Client): ${totalReceived}`);
    console.log(`Messages Sent (Client -> Server): ${totalSent}`);
    if (BIDIRECTIONAL) {
      console.log(`Average Client Write Latency: ${avgPostLatency} ms`);
    }
    console.log(`Server RSS Memory Profile:`);
    console.log(`- Min RSS: ${minRss} MB`);
    console.log(`- Max RSS: ${maxRss} MB`);
    console.log(`- Avg RSS: ${avgRss} MB`);

    console.log('\n[SUMMARY_JSON]', JSON.stringify({
      protocol: PROTOCOL,
      connections: totalConnected,
      errors: totalErrors,
      received: totalReceived,
      sent: totalSent,
      avgLatency: avgPostLatency,
      minRss,
      maxRss,
      avgRss,
    }));

    for (const id in cluster.workers) {
      cluster.workers[id].send({ type: 'shutdown' });
    }

    setTimeout(() => process.exit(0), 1000);
  }, DURATION * 1000);

} else {
  // WORKER PROCESS
  const workerId = parseInt(process.env.WORKER_ID || '0');
  const targetConnections = parseInt(process.env.WORKER_CONNECTIONS || '1000');
  const protocol = process.env.PROTOCOL || 'ws';
  const port = parseInt(process.env.PORT || '8080');
  const host = process.env.HOST || 'localhost';
  const bidirectional = process.env.BIDIRECTIONAL === 'true';
  const writeInterval = parseInt(process.env.WRITE_INTERVAL || '5000');

  const stats = {
    connected: 0,
    errors: 0,
    received: 0,
    sent: 0,
    postLatencySum: 0,
    postLatencyCount: 0,
  };

  setInterval(() => {
    if (process.send) {
      process.send({ type: 'stats', data: stats });
    }
  }, 1000);

  const clients = [];

  function getLocalIp(index) {
    return LOCAL_IPS[index % LOCAL_IPS.length];
  }

  function sendHttpPost(clientId) {
    const startTime = Date.now();
    const payload = JSON.stringify({ type: 'client-message', id: clientId, timestamp: startTime });

    const req = http.request({
      hostname: host,
      port: port,
      path: '/post',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(payload)
      }
    }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        const latency = Date.now() - startTime;
        stats.postLatencySum += latency;
        stats.postLatencyCount++;
        stats.sent++;
      });
    });

    req.on('error', () => {
      stats.errors++;
    });

    req.write(payload);
    req.end();
  }

  function createWebSocketClient(id) {
    let isConnected = false;
    let writeTimer = null;

    const options = {
      family: 4,
      perMessageDeflate: false,
      handshakeTimeout: 30000,
    };

    const ws = new WebSocket(`ws://${host}:${port}/ws`, options);

    ws.on('open', () => {
      isConnected = true;
      stats.connected++;
      if (bidirectional) {
        writeTimer = setInterval(() => {
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: 'ping', id }));
            stats.sent++;
          }
        }, writeInterval);
      }
    });

    ws.on('ping', (data) => {
      try {
        if (ws.readyState === WebSocket.OPEN) {
          ws.pong(data);
        }
      } catch (e) {}
    });

    ws.on('message', () => {
      stats.received++;
    });

    ws.on('close', () => {
      if (isConnected) {
        isConnected = false;
        stats.connected = Math.max(0, stats.connected - 1);
      }
      if (writeTimer) clearInterval(writeTimer);
    });

    ws.on('error', () => {
      stats.errors++;
    });

    clients.push({
      close: () => {
        if (writeTimer) clearInterval(writeTimer);
        try {
          ws.terminate();
        } catch (e) {}
      }
    });
  }

  function createSseHttp1Client(id) {
    let writeTimer = null;

    const req = http.get(`http://${host}:${port}/events`, {
      headers: {
        'Accept': 'text/event-stream',
        'Cache-Control': 'no-cache',
      }
    }, (res) => {
      if (res.statusCode === 200) {
        stats.connected++;
      } else {
        stats.errors++;
        return;
      }

      let buffer = '';
      res.on('data', (chunk) => {
        buffer += chunk.toString();
        const lines = buffer.split('\n');
        buffer = lines.pop();

        for (const line of lines) {
          if (line.startsWith('data:')) {
            stats.received++;
          }
        }
      });

      if (bidirectional) {
        writeTimer = setInterval(() => {
          sendHttpPost(id);
        }, writeInterval);
      }
    });

    req.on('error', () => {
      stats.errors++;
    });

    clients.push({
      close: () => {
        if (writeTimer) clearInterval(writeTimer);
        req.destroy();
        stats.connected = Math.max(0, stats.connected - 1);
      }
    });
  }

  let h2Session = null;
  function createSseHttp2Client(id) {
    let writeTimer = null;

    if (!h2Session || h2Session.destroyed) {
      h2Session = http2.connect(`http://${host}:${port}`);
      h2Session.on('error', () => {
        stats.errors++;
      });
    }

    const req = h2Session.request({
      ':path': '/events',
      ':method': 'GET',
      'accept': 'text/event-stream',
    });

    req.on('response', (headers) => {
      if (headers[':status'] === 200) {
        stats.connected++;
      } else {
        stats.errors++;
      }
    });

    let buffer = '';
    req.on('data', (chunk) => {
      buffer += chunk.toString();
      const lines = buffer.split('\n');
      buffer = lines.pop();

      for (const line of lines) {
        if (line.startsWith('data:')) {
          stats.received++;
        }
      }
    });

    req.on('error', () => {
      stats.errors++;
    });

    req.on('close', () => {
      stats.connected = Math.max(0, stats.connected - 1);
      if (writeTimer) clearInterval(writeTimer);
    });

    if (bidirectional) {
      writeTimer = setInterval(() => {
        if (h2Session && !h2Session.destroyed) {
          const startTime = Date.now();
          const payload = JSON.stringify({ type: 'client-message', id, timestamp: startTime });
          const postReq = h2Session.request({
            ':path': '/post',
            ':method': 'POST',
            'content-type': 'application/json',
          });

          postReq.on('response', (headers) => {
            if (headers[':status'] === 200) {
              stats.sent++;
              const latency = Date.now() - startTime;
              stats.postLatencySum += latency;
              stats.postLatencyCount++;
            } else {
              stats.errors++;
            }
          });
          postReq.on('error', () => {
            stats.errors++;
          });
          postReq.write(payload);
          postReq.end();
        }
      }, writeInterval);
    }

    clients.push({
      close: () => {
        if (writeTimer) clearInterval(writeTimer);
        req.close();
      }
    });
  }

  // Connection ramp-up in paced batches
  let launched = 0;
  const launchBatchSize = 50;
  const launchIntervalMs = 10;

  const launchTimer = setInterval(() => {
    const batch = Math.min(launchBatchSize, targetConnections - launched);
    for (let i = 0; i < batch; i++) {
      const id = workerId * 100000 + launched + i;
      if (protocol === 'ws') {
        createWebSocketClient(id);
      } else if (protocol === 'sse-http1') {
        createSseHttp1Client(id);
      } else if (protocol === 'sse-http2') {
        createSseHttp2Client(id);
      }
    }
    launched += batch;
    if (launched >= targetConnections) {
      clearInterval(launchTimer);
    }
  }, launchIntervalMs);

  process.on('message', (msg) => {
    if (msg.type === 'shutdown') {
      clearInterval(launchTimer);
      clients.forEach(c => c.close());
      if (h2Session && !h2Session.destroyed) {
        try {
          h2Session.destroy();
        } catch (e) {}
      }
      process.exit(0);
    }
  });
}
