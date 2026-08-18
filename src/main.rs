use std::path::PathBuf;

use tracing::{error,info};
use clap::{Parser, Subcommand};
use redb::{Database, Error};

mod server;
use server::{serve};
mod core;
use core::{
  App, Record, Deleted, SysError
};

#[derive(Subcommand)]
enum Command {
  Run,
  //Rebuild,
  //Rebalance
}

const DEFAULT_PORT:u16 = 4545;
const DEFAULT_NREPLICAS:usize = 3;
const DEFAULT_NSUB:usize = 10;
const DEFAULT_VOLTIMEOUT:usize = 1000;

#[derive(Parser)]
struct Args {
  #[arg(long, default_value_t = DEFAULT_PORT, help = "port to run crabmap on")]
  port:u16,
  #[arg(long, default_value = "/tmp/index.db", help = "path to the db file")]
  dbfile:PathBuf,
  #[arg(long, default_value_t = DEFAULT_NREPLICAS, help = "num of replicas")]
  nreplicas:usize,
  #[arg(long, default_value_t = DEFAULT_NSUB, help = "num of subvolumes/drives")]
  nsub:usize,
  #[arg(long, default_value = "", help = "list of volume servers comma separated")]
  pvolumes:String,
  #[arg(long, default_value_t = DEFAULT_VOLTIMEOUT, help = "request timeout for volume servers - in miliseconds")]
  voltimeout:usize,
  #[command(subcommand)]
  command:Option<Command>,
}

fn test_db_rec(app:&App) -> Result<(),SysError> {
  info!("test_db_rec: called!");
  app.ensure_table()?;
  info!("test_db_rec: Database created!");
  let sample_rec = Record {
    replica_volumes: vec!["v1".to_string(), "v2".to_string()],
    deleted: Deleted(0),
    content_hash: "abc123".to_string(),
  };
  let sample_key = String::from("sample_file_key");
  let fetched_rec = match app.get_record(&sample_key) {
    Ok(record) => record,
    Err(SysError::RecordNotFound) => {
      app.put_record(&sample_key, &sample_rec)?;
      info!("test_db_rec: record inserted!");
      sample_rec
    }
    Err(err) => return Err(err),
  };
  info!("test_db_rec: {:?}", fetched_rec);
  Ok(())
}

fn open_db(dbfile:&PathBuf) -> Result<Database,Error> {
  let db = Database::create(dbfile)?;
  Ok(db)
}

#[tokio::main]
async fn main() {
  tracing_subscriber::fmt()
    .with_writer(std::io::stdout)
    .init();
  let args = Args::parse();
  let port = args.port;
  let dbfile = args.dbfile;
  let nreplicas = args.nreplicas;
  let nsub = args.nsub;
  let pvolumes = args.pvolumes;
  let voltimeout = args.voltimeout;
  info!("main: dbfile = {}", dbfile.display());
  info!("main: nreplicas = {nreplicas}, nsub = {nsub}, voltimeout = {voltimeout}");
  let volumes:Vec<String> = pvolumes
    .split(",")
    .map(String::from)
    .collect();
  let vlen:usize = volumes.len();
  if vlen <= 0 {
    error!("!main: no. of volumes <= 0, you must have atleast one volume server");
    return;
  }
  if nreplicas <= 0 {
    error!("main: you need to have atleast one replica");
    return;
  }
  if nreplicas > vlen {
    error!("main: error - you need to have atleast as many volume servers as replicas");
    error!("main: {} > {}", nreplicas, vlen);
    return;
  }
  let db = open_db(&dbfile).unwrap();
  let app = App {
    volumes: volumes,
    nreplicas: nreplicas,
    nsub: nsub,
    voltimeout: voltimeout,
    db: &db,
  };
  let _ = test_db_rec(&app);
  match args.command {
    Some(Command::Run) => {
      serve(port).await;
    }
    None => {
      error!("main: no command provided! available: `run, rebuild, rebalance`");
    }
  }
}

