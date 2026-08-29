# Devil's Advocate: WebSockets vs. SSE at Scale (TS, Go, Rust)

This Proof of Concept (PoC) is designed to critically evaluate the claims made in the popular article *"WebSockets vs SSE at 100K Connections: SSE Used 40% Less Memory"*. 

The main objective is to showcase the structural vulnerabilities, hidden costs, and operational challenges of Server-Sent Events (SSE) compared to WebSockets in real-world scenarios across three programming runtimes: **TypeScript (Node.js)**, **Go**, and **Rust**.

## Repository Structure

```text
├── typescript/            # TypeScript (Node.js) Server Implementation
├── go/                    # Go Server Implementation
├── rust/                  # Rust Server Implementation (Axum)
├── clients/               # Load Testing Client Script (Node.js)
├── scripts/               # Automation Script for Benchmarking
├── ARTICLE.md             # Critical analysis and findings (Devil's Advocate)
└── README.md              # This documentation
```

## Evaluated SSE Shortcomings (The Devil's Advocate Thesis)

1. **HTTP/2 Stream Memory Overhead:** To bypass the 6-connection-per-domain limit on HTTP/1.1, SSE must run over HTTP/2. However, HTTP/2 introduces substantial memory overhead per stream on the server (dynamic HPACK compression tables, stream state tracking).
2. **Bidirectional Communication Penalty:** Since SSE is unidirectional (server-to-client), any client-to-server data (e.g., changing filters, acknowledgment, user actions) must be sent via a separate HTTP POST request. This initiates a full HTTP request lifecycle (TLS handshake, parsing large headers like Cookies/User-Agent, allocating request/response objects), causing memory/CPU spikes.
3. **Half-Open Connection Leaks:** WebSockets have native Ping/Pong control frames at the protocol layer (RFC 6455). SSE is a standard chunked HTTP stream. Without custom application-level heartbeat setups, silently disconnected clients (e.g., losing signal) leak server resources and file descriptors indefinitely.
4. **Middlebox/Proxy Buffering:** Standard proxies (Nginx, Cloudflare, ALB) buffer HTTP chunked transfers by default. Bypassing this requires disabling buffering (`X-Accel-Buffering: no` on Nginx) which disables optimizations for other HTTP traffic. WebSockets bypass this naturally.

---

## How to Run the Benchmarks

### Prerequisites
- Node.js (v20+)
- Go (v1.22+)
- Rust & Cargo

### Setup Steps

1. **Prepare TypeScript Server:**
   ```bash
   cd typescript
   npm install
   npm run build
   cd ..
   ```

2. **Prepare Go Server:**
   ```bash
   cd go
   go mod tidy
   go build -o go-server main.go
   cd ..
   ```

3. **Prepare Rust Server:**
   ```bash
   cd rust
   cargo build --release
   cd ..
   ```

4. **Run Automation Benchmark Suite:**
   This script runs benchmarks for all languages and protocols (WS, SSE HTTP/1.1, SSE HTTP/2) under unidirectional and bidirectional workloads.
   ```bash
   chmod +x scripts/benchmark.sh
   ./scripts/benchmark.sh
   ```

The results will be written to `benchmark-results.txt` at the root of the repository.
