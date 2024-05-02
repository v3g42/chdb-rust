use std::{env, path::PathBuf};

use chdb_rust::{ChdbResult, Error};
use chdb_rust::{LocalResultV2, Session, SessionBuilder};
fn initialize(session: &Session) -> Option<LocalResultV2> {
    session.execute(
        "
          CREATE DATABASE IF NOT EXISTS demo;

          CREATE TABLE IF NOT EXISTS demo.stories (
            id UInt32,
            name String
          ) 
          ENGINE = MergeTree
          PARTITION BY id
          ORDER BY id
          ",
    )
}
fn _insert(session: &Session, current_dir: PathBuf) -> Option<LocalResultV2> {
    let filename = current_dir
        .join("core/examples/test.csv")
        .to_string_lossy()
        .to_string();

    let query = format!(
        "
        set input_format_parquet_allow_missing_columns = 1;
        INSERT INTO demo.stories SELECT * FROM file('{filename}', CSV);
        "
    );
    session.execute(query)
}

fn print_query_result(v: Option<LocalResultV2>) -> ChdbResult<()> {
    if v.is_none() {
        println!("Got empty");
        return Ok(());
    };
    let v = v.unwrap();

    if v.has_error() {
        return Err(Error::InvalidData(v.error_message().to_string()));
    };

    let result = String::from_utf8(v.buf().to_vec()).unwrap();
    println!("{}", result);
    Ok(())
}

fn main() {
    let current_dir = env::current_dir().unwrap();

    let session = SessionBuilder::new()
        .format("JSON")
        .data_path(current_dir.join(".langdb/data"))
        .udf_path(current_dir.join("udfs/src"))
        .build()
        .unwrap();

    print_query_result(initialize(&session)).unwrap();
    // print_query_result(_insert(&session, current_dir)).unwrap();

    let query = format!("Select id, name from demo.stories;");
    // let query = format!("Select id, name, embedding(name) from demo.stories;");

    print_query_result(session.execute(query)).unwrap();
}
