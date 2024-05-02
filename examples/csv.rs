use chdb_rust::{query, ChdbResult, Error};

use std::env;

fn main() -> std::io::Result<()> {
    let filename = env::current_dir().unwrap().join("core/examples/test.csv");
    let filename = filename.to_string_lossy().to_string();
    let csv = query_file(filename).unwrap();
    println!("{csv}");
    Ok(())
}

fn query_file(filename: String) -> ChdbResult<String> {
    let q = format!("SELECT * from file(\"{filename}\")");
    let v = query(&q, "CSV", None);

    if v.is_none() {
        return Ok(String::from("Got empty"));
    };

    let v = v.unwrap();

    if v.has_error() {
        return Err(Error::InvalidData(v.error_message().to_string()));
    };

    let s = String::from_utf8(v.buf().to_vec())?;
    Ok(s)
}
