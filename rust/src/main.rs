use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse,
    },
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use futures_util::{sink::SinkExt, stream::StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    convert::Infallible,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::broadcast;
use tokio_stream::{wrappers::BroadcastStream, Stream};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "ws")]
    server: String,

    #[arg(short, long, default_value_t = 8080)]
    port: u16,
}

struct AppState {
    active_connections: AtomicUsize,
    tx: broadcast::Sender<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ClientMessage {
    r#type: String,
    value: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    println!("Starting Rust Server: mode={}, port={}", args.server, args.port);

    // Broadcast channel for SSE streams
    let (tx, _) = broadcast::channel::<String>(1000);
    let state = Arc::new(AppState {
        active_connections: AtomicUsize::new(0),
        tx: tx.clone(),
    });

    // Spawn a producer task that generates fake price ticks every 1 second
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            let price = rand::random::<f64>() * 100.0;
            let tick = json!({
                "type": "price",
                "value": format!("{:.2}", price),
                "timestamp": chrono::Utc::now().timestamp_millis()
            })
            .to_string();
            let _ = tx_clone.send(tick);
        }
    });

    // Spawn memory monitor
    let state_clone = state.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            println!(
                "[MEM] Rust Clients: {}",
                state_clone.active_connections.load(Ordering::Relaxed)
            );
        }
    });

    let app = Router::new()
        .route("/health", get(handle_health))
        .route("/post", post(handle_post));

    let app = match args.server.as_str() {
        "ws" => app.route("/ws", get(handle_ws_upgrade)).with_state(state),
        "sse1" | "sse2" => app.route("/events", get(handle_sse)).with_state(state),
        _ => panic!("Unknown server type: {}", args.server),
    };

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", args.port))
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn handle_health() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

async fn handle_post(Json(payload): Json<serde_json::Value>) -> impl IntoResponse {
    Json(json!({ "status": "ok", "echoed": payload }))
}

async fn handle_ws_upgrade(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, state))
}

async fn handle_ws(socket: WebSocket, state: Arc<AppState>) {
    state.active_connections.fetch_add(1, Ordering::Relaxed);
    let (mut sender, mut receiver) = socket.split();

    // Spawn periodic price update task for this socket
    let (tx_ws, rx_ws) = tokio::sync::mpsc::channel::<String>(100);
    let tx_clone = state.tx.clone();
    
    // Subscribe to global price tick broadcast
    let mut rx_broadcast = tx_clone.subscribe();
    
    let tx_ws_clone = tx_ws.clone();
    tokio::spawn(async move {
        while let Ok(msg) = rx_broadcast.recv().await {
            if tx_ws_clone.send(msg).await.is_err() {
                break;
            }
        }
    });

    let state_clone = state.clone();
    
    // Task to send messages to the client
    let mut tx_ws_stream = tokio_stream::wrappers::ReceiverStream::new(rx_ws);
    tokio::spawn(async move {
        while let Some(msg) = tx_ws_stream.next().await {
            if sender.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
        state_clone.active_connections.fetch_sub(1, Ordering::Relaxed);
    });

    // Task to read messages from the client
    while let Some(Ok(msg)) = receiver.next().await {
        if let Message::Text(text) = msg {
            if let Ok(client_msg) = serde_json::from_str::<ClientMessage>(&text) {
                if client_msg.r#type == "ping" {
                    // Send pong back
                    let _ = tx_ws.send(json!({ "type": "pong" }).to_string()).await;
                }
            }
        }
    }
}

async fn handle_sse(
    State(state): State<Arc<AppState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    state.active_connections.fetch_add(1, Ordering::Relaxed);
    
    let rx_broadcast = state.tx.subscribe();
    let state_clone = state.clone();
    
    let stream = BroadcastStream::new(rx_broadcast)
        .filter_map(|res| async {
            match res {
                Ok(msg) => Some(Ok(Event::default().data(msg))),
                Err(_) => None,
            }
        })
        .map(move |res| {
            res
        });

    // Monitor stream dropping
    let stream_with_cleanup = tokio_stream::StreamExt::map(stream, move |item| {
        item
    });
    
    struct Cleanup {
        state: Arc<AppState>,
    }
    
    impl Drop for Cleanup {
        fn drop(&mut self) {
            self.state.active_connections.fetch_sub(1, Ordering::Relaxed);
        }
    }
    
    let _cleanup = Cleanup {
        state: state_clone,
    };

    // To make sure _cleanup is moved into the stream closure so it drops when stream drops:
    let stream = tokio_stream::StreamExt::map(stream_with_cleanup, move |item| {
        let _keep_alive = &_cleanup;
        item
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}
