use axum::{extract::{Path, Request}, routing::any, Router};
use tracing::{info};

async fn handle_put(key:&str) -> &'static str {
  "PUT /: Baby girl..."
}

async fn handle_get(key:&str) -> &'static str {
  "GET /: Hey.."
}

async fn handle_post(key:&str) -> &'static str {
  "POST /: How you doin?"
}

async fn handle_delete(key:&str) -> &'static str {
  "DELETE /: I need ya!"
}

async fn handle_unlink(key:&str) -> &'static str {
  "UNLINK /: I need ya!"
}

async fn handle_rebalance(key:&str) -> &'static str {
  "REBALANCE /: I wanna be inside ya..."
}

async fn dispatch(Path(key):Path<String>, request:Request) -> &'static str {
  match request.method().as_str() {
    "GET" => handle_get(&key).await,
    "POST" => handle_post(&key).await,
    "PUT" => handle_put(&key).await,
    "DELETE" => handle_delete(&key).await,
    "UNLINK" => handle_unlink(&key).await,
    "REBALANCE" => handle_rebalance(&key).await,
    _ => "Method not allowed",
  }
}

pub async fn serve(port:u16) {
  let app = Router::new().route("/{*.key}", any(dispatch));
  let listener = tokio::net::TcpListener::bind(format!("localhost:{port}"))
    .await
    .unwrap();
  info!("serve: server running on http://localhost:{port}");
  axum::serve(listener, app).await.unwrap();
}

