use redb::{Database, ReadableDatabase, TableDefinition};

use crate::core::{Record, SysError, from_record, to_record};

const TABLE:TableDefinition<String,String> = TableDefinition::new("path_map");

#[derive(Debug)]
pub struct App<'a> {
  pub volumes: Vec<String>,
  pub nsub: usize,
  pub nreplicas: usize,
  pub voltimeout: usize,
  pub db: &'a Database
}

pub fn ensure_table<'a>(app:&'a App) -> Result<&'a Database,SysError> {
  let db = app.db;
  {
    let write_txn = db.begin_write()?;
    let table = write_txn.open_table(TABLE)?;
    drop(table);
    write_txn.commit()?;
  }
  Ok(db)
}

pub fn get_record(app:&App, key:&String) -> Result<Record,SysError> {
  let db = app.db;
  let read_txn = db.begin_read()?;
  let table = read_txn.open_table(TABLE)?;
  let record = table.get(key)?.ok_or(SysError::RecordNotFound)?;
  let json = record.value().to_string();
  to_record(&json)
}

pub fn put_record(app:&App, key:&String, rec:&Record) -> Result<(),SysError> {
  let db = app.db;
  let write_txn = db.begin_write()?;
  {
    let mut table = write_txn.open_table(TABLE)?;
    let json:String = from_record(rec)?;
    table.insert(key, json)?;
  }
  write_txn.commit()?;
  Ok(())
}

