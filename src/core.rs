use thiserror;
use serde::{Deserialize, Serialize};
use tracing::error;
use redb::{Database, ReadableDatabase, TableDefinition};

const TABLE:TableDefinition<String,String> = TableDefinition::new("path_map");

#[derive(Debug, Serialize, Deserialize)]
pub struct Deleted(pub i32);

#[derive(Debug)]
pub struct App {
  pub volumes: Vec<String>,
  pub nsub: usize,
  pub nreplicas: usize,
  pub voltimeout: usize,
  pub db: Database
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

impl App {
  pub fn ensure_table(&self) -> Result<&Database,SysError> {
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
}

