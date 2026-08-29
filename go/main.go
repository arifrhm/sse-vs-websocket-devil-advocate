package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

var (
	serverType = flag.String("server", "ws", "Type of server to run (ws, sse1, sse2)")
	port       = flag.Int("port", 8080, "Port to run the server on")
)

// Global map to track active SSE clients
type ClientMap struct {
	sync.Mutex
	clients map[chan string]bool
}

var sseClients = ClientMap{
	clients: make(map[chan string]bool),
}

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	wsClientsMu sync.Mutex
	wsClientsCount int
)

func main() {
	flag.Parse()

	// Memory reporting goroutine
	go func() {
		for {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			var activeClients int
			if *serverType == "ws" {
				wsClientsMu.Lock()
				activeClients = wsClientsCount
				wsClientsMu.Unlock()
			} else {
				sseClients.Lock()
				activeClients = len(sseClients.clients)
				sseClients.Unlock()
			}
			fmt.Printf("[MEM] Alloc: %.2f MB, TotalAlloc: %.2f MB, Sys: %.2f MB, NumGC: %d, Clients: %d\n",
				float64(m.Alloc)/1024/1024,
				float64(m.TotalAlloc)/1024/1024,
				float64(m.Sys)/1024/1024,
				m.NumGC,
				activeClients,
			)
			time.Sleep(5 * time.Second)
		}
	}()

	// Periodic data generator for SSE
	go func() {
		for {
			time.Sleep(1 * time.Second)
			data := fmt.Sprintf(`{"type":"price","value":"%.2f","timestamp":%d}`, rand.Float64()*100, time.Now().UnixMilli())
			
			sseClients.Lock()
			for ch := range sseClients.clients {
				select {
				case ch <- data:
				default:
					// If channel is full, drop or skip
				}
			}
			sseClients.Unlock()
		}
	}()

	mux := http.NewServeMux()

	// Health endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "ok",
			"alloc":  m.Alloc,
			"sys":    m.Sys,
		})
	})

	switch *serverType {
	case "ws":
		mux.HandleFunc("/ws", handleWebSocket)
		fmt.Printf("Starting Go WebSocket server on port %d\n", *port)
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), mux))

	case "sse1":
		mux.HandleFunc("/events", handleSSE)
		mux.HandleFunc("/post", handlePost)
		fmt.Printf("Starting Go SSE HTTP/1.1 server on port %d\n", *port)
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), mux))

	case "sse2":
		mux.HandleFunc("/events", handleSSE)
		mux.HandleFunc("/post", handlePost)
		fmt.Printf("Starting Go SSE HTTP/2 (H2C) server on port %d\n", *port)
		
		h2s := &http2.Server{
			MaxConcurrentStreams: 100000,
		}
		h1s := &http.Server{
			Addr:    fmt.Sprintf(":%d", *port),
			Handler: h2c.NewHandler(mux, h2s),
		}
		log.Fatal(h1s.ListenAndServe())

	default:
		log.Fatalf("Unknown server type: %s", *serverType)
	}
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade error:", err)
		return
	}
	defer conn.Close()

	wsClientsMu.Lock()
	wsClientsCount++
	wsClientsMu.Unlock()

	defer func() {
		wsClientsMu.Lock()
		wsClientsCount--
		wsClientsMu.Unlock()
	}()

	// Ping/pong check
	conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(15 * time.Second))
		return nil
	})

	var writeMu sync.Mutex
	writeJSON := func(data interface{}) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return conn.WriteJSON(data)
	}
	writeMessage := func(messageType int, data []byte) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return conn.WriteMessage(messageType, data)
	}

	// Keepalive & ping sender
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := writeMessage(websocket.PingMessage, nil); err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Periodic updates sender
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				data := map[string]interface{}{
					"type":      "price",
					"value":     fmt.Sprintf("%.2f", rand.Float64()*100),
					"timestamp": time.Now().UnixMilli(),
				}
				if err := writeJSON(data); err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Client to server reader
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			break
		}
		// Simulate payload handling
		var msg map[string]interface{}
		if err := json.Unmarshal(message, &msg); err == nil {
			if msg["type"] == "ping" {
				writeJSON(map[string]string{"type": "pong"})
			}
		}
	}
}

func handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	fmt.Fprintf(w, ": ok\n\n")
	flusher.Flush()

	messageChan := make(chan string, 10)
	sseClients.Lock()
	sseClients.clients[messageChan] = true
	sseClients.Unlock()

	defer func() {
		sseClients.Lock()
		delete(sseClients.clients, messageChan)
		sseClients.Unlock()
		close(messageChan)
	}()

	// Reconnection or close detector
	notify := r.Context().Done()

	for {
		select {
		case msg := <-messageChan:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		case <-notify:
			return
		}
	}
}

func handlePost(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
		"echoed": data,
	})
}
