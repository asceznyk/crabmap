use thiserror;
use serde::{Deserialize, Serialize};
use tracing::error;

#[derive(Debug, Serialize, Deserialize)]
pub struct Deleted(pub i32);

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
  #[error("Record not found")]
  RecordNotFound,
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

