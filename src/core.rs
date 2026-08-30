use thiserror;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::{error, info};
use redb::{Database, ReadableDatabase, TableDefinition};
use axum::{
  Json,
  http::StatusCode,
  response::{IntoResponse, Response},
  extract::{Request}
};
use md5::{Digest, Md5};

const TABLE:TableDefinition<String,String> = TableDefinition::new("path_map");

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Deleted(pub i32);

impl Deleted {
  pub const NO:Deleted = Deleted(0);
  pub const SOFT:Deleted = Deleted(1);
  pub const HARD:Deleted = Deleted(2);
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Record {
  pub replica_volumes: Vec<String>,
  pub deleted: Deleted,
  pub content_hash: String
}

#[derive(Debug, thiserror::Error)]
pub enum SysError {
  #[error("transaction error: {0}")]
  Transaction(#[from] redb::TransactionError),
  #[error("table error: {0}")]
  Table(#[from] redb::TableError),
  #[error("storage error: {0}")]
  Storage(#[from] redb::StorageError),
  #[error("commit error: {0}")]
  Commit(#[from] redb::CommitError),
  #[error("JSON error: {0}")]
  Json(#[from] serde_json::Error),
  #[error("not found")]
  NotFound,
  #[error("record not found")]
  RecordNotFound,
  #[error("internal server error")]
  Internal,
}

impl IntoResponse for SysError {
  fn into_response(self) -> Response {
    match self {
      SysError::NotFound => (
        StatusCode::NOT_FOUND,
        Json(json!({
          "error": "not found"
        })),
      ).into_response(),
      SysError::Internal => (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
          "error": "internal server error"
        })),
      ).into_response(),
      SysError::RecordNotFound => (
        StatusCode::NOT_FOUND,
        Json(json!({
          "error": "record not found"
        })),
      ).into_response(),
      _ => (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
          "error": "internal server error"
        })),
      ).into_response(),
    }
  }
}

pub fn to_record(json_str:&str) -> Result<Record,SysError> {
  let rec:Record = match serde_json::from_str(json_str) {
    Ok(rec) => rec,
    Err(err) => {
      error!("to_record: error in from_str serde_json!");
      return Err(err.into());
    }
  };
  Ok(rec)
}

pub fn from_record(rec:&Record) -> Result<String,SysError> {
  let json:String = match serde_json::to_string(rec) {
    Ok(json) => json,
    Err(err) => {
      error!("from_record: error in to_string serde_json!");
      return Err(err.into());
    }
  };
  Ok(json)
}

fn hash_key_volume(key:&[u8], volume:&str) -> String {
  let mut hasher = Md5::new();
  hasher.update(key);
  hasher.update(volume.as_bytes());
  format!("{:x}", hasher.finalize())
}

#[derive(Debug)]
struct SortVol<'a> {
  volume: &'a str,
  hash: String,
}

pub fn select_volumes_by_key(
  key:&String,
  volumes:&[String],
  nreplicas:usize,
  nsub:usize
) -> Vec<String> {
  let mut sortvols = Vec::<SortVol>::new();
  for volume in volumes {
    let hash = hash_key_volume(key.as_bytes(), volume);
    sortvols.push(SortVol { volume, hash });
  }
  sortvols.sort_by(|a, b| a.hash.cmp(&b.hash));
  info!("select_volumes_by_key: sortvols = {:?}", sortvols);
  vec!["dummy".to_string()] //TODO: return the sortvols with nsub hashing
}

#[derive(Debug)]
pub struct App {
  pub volumes: Vec<String>,
  pub nsub: usize,
  pub nreplicas: usize,
  pub voltimeout: usize,
  pub db: Database
}

impl App {
  pub fn ensure_table(&self) -> Result<&Database,SysError> {
    info!("app.ensure_table: called!");
    let db = &self.db;
    {
      let write_txn = db.begin_write()?;
      let table = write_txn.open_table(TABLE)?;
      drop(table);
      write_txn.commit()?;
    }
    Ok(db)
  }
  pub fn get_record(&self, key:&String) -> Result<Record,SysError> {
    let db = &self.db;
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(TABLE)?;
    let record = table.get(key)?.ok_or(SysError::RecordNotFound)?;
    let json = record.value().to_string();
    to_record(&json)
  }
  pub fn put_record(&self, key:&String, rec:&Record) -> Result<(),SysError> {
    let db = &self.db;
    let write_txn = db.begin_write()?;
    {
      let mut table = write_txn.open_table(TABLE)?;
      let json:String = from_record(rec)?;
      table.insert(key, json)?;
    }
    write_txn.commit()?;
    Ok(())
  }
  pub fn write_to_replicas(
    &self,
    key:&String,
    req:Request
  ) -> Result<(),SysError> {
    select_volumes_by_key(key, &self.volumes, self.nreplicas, self.nsub);
    /*let sample_rec = Record {
      replica_volumes: vec!["v1".to_string(), "v2".to_string()],
      deleted: Deleted::NO,
      content_hash: "abc123".to_string(),
    };*/
    //self.put_record(&key.to_string(), &sample_rec)?;
    Ok(())
  }
}

