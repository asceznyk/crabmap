use std::sync::Arc;

use axum::{extract::{Path, State, Request}, routing::any, Router};
use tracing::{info};

use crate::core::{App, Record, Deleted, SysError};

async fn handle_put(app:&App, key:&str) -> &'static str {
  let sample_rec = Record {
    replica_volumes: vec!["v1".to_string(), "v2".to_string()],
    deleted: Deleted(0),
    content_hash: "abc123".to_string(),
  };
  app.put_record(&key.to_string(), &sample_rec);
  "PUT /: Baby girl..."
}

async fn handle_get(app:&App, key:&str) -> &'static str {
  let rec = match app.get_record(&key.to_string()) {
    Ok(rec) => rec,
    Err(err) => {
      return "Error!";
    }
  };
  info!("handle_get: {:?}", rec);
  "GET /: Hey.."
}

async fn handle_post(app:&App, key:&str) -> &'static str {
  "POST /: How you doin?"
}

async fn handle_delete(app:&App, key:&str) -> &'static str {
  "DELETE /: I need ya!"
}

async fn handle_unlink(app:&App, key:&str) -> &'static str {
  "UNLINK /: I need ya!"
}

async fn handle_rebalance(app:&App, key:&str) -> &'static str {
  "REBALANCE /: I wanna be inside ya..."
}

async fn dispatch(
  State(app):State<Arc<App>>,
  Path(key):Path<String>,
  request:Request,
) -> &'static str {
  match request.method().as_str() {
    "GET" => handle_get(&app, &key).await,
    "POST" => handle_post(&app, &key).await,
    "PUT" => handle_put(&app, &key).await,
    "DELETE" => handle_delete(&app, &key).await,
    "UNLINK" => handle_unlink(&app, &key).await,
    "REBALANCE" => handle_rebalance(&app, &key).await,
    _ => "Method not allowed",
  }
}

pub async fn serve(app:Arc<App>, port:u16) {
  let aroute = Router::new()
    .route("/{*.key}", any(dispatch))
    .with_state(app);
  let listener = tokio::net::TcpListener::bind(format!("localhost:{port}"))
    .await
    .unwrap();
  info!("serve: server running on http://localhost:{port}");
  axum::serve(listener, aroute).await.unwrap();
}

