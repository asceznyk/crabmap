use std::sync::Arc;

use axum::{
  extract::{Path, State, Request},
  http::{StatusCode},
  response::{IntoResponse, Response},
  Json,
  Router,
  routing::any
};
use tracing::{error,info};
use serde_json::{json, Value};

use crate::core::{App, Record, Deleted, SysError};

async fn handle_put(
  app:&App,
  key:&str,
  req:Request,
) -> Result<(StatusCode, Json<Value>), SysError> {
  info!("handle_put: called!");
  let content_length = req
    .headers()
    .get(axum::http::header::CONTENT_LENGTH)
    .and_then(|v| v.to_str().ok())
    .and_then(|v| v.parse::<usize>().ok());
  if content_length == Some(0) {
    return Ok((
      StatusCode::LENGTH_REQUIRED,
      Json(json!({
        "error": "Content-Length is required!"
      })),
    ));
  }
  let rec = match app.get_record(&key.to_string()) {
    Ok(rec) => Some(rec),
    Err(SysError::RecordNotFound) => None,
    Err(err) => {
      error!("handle_put: Err(err) = {:?}!", err);
      return Err(err);
    },
  };
  if let Some(rec) = rec {
    if rec.deleted == Deleted::NO {
      return Ok((
        StatusCode::FORBIDDEN,
        Json(json!({
          "error": "PUTting into an existing key!"
        })),
      ));
    }
  }
  let _ = app.write_to_replicas(&key.to_string(), req).await;
  Ok((
    StatusCode::CREATED,
    Json(json!({
      "status": "success",
      "key": key
    })),
  ))
}

async fn handle_get(
  app:&App,
  key:&str,
) -> Result<(StatusCode, Json<Value>), SysError> { //TODO: rewrite this!
  let rec = match app.get_record(&key.to_string()) {
    Ok(rec) => rec,
    Err(err) => {
      return Err(err);
    }
  };
  Ok((
    StatusCode::OK,
    Json(json!({
      "status": "success",
      "value": rec
    })),
  ))
}

/*async fn handle_post(app:&App, key:&str) -> &'static str {
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
}*/

async fn dispatch(
  State(app):State<Arc<App>>,
  Path(key):Path<String>,
  req:Request,
) -> Response {
  info!("dispatch: request routing...");
  match req.method().as_str() {
    "PUT" => {
      handle_put(&app, &key, req).await.into_response()
    },
    "GET" => {
      handle_get(&app, &key).await.into_response()
    },
    _ => {
      (
        StatusCode::METHOD_NOT_ALLOWED,
        Json(json!({
          "error": "method not allowed"
        })),
      ).into_response()
    },
  }
}

pub async fn serve(app:Arc<App>, port:u16) -> Result<(),SysError> {
  let _ = app.ensure_table()?;
  let aroute = Router::new()
    .route("/{*key}", any(dispatch))
    .with_state(app);
  let listener = tokio::net::TcpListener::bind(format!("localhost:{port}"))
    .await
    .unwrap();
  info!("serve: listening on http://localhost:{port}");
  axum::serve(listener, aroute).await.unwrap();
  Ok(())
}

