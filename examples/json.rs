use chdb_rust::{query, ChdbResult, Error};

use serde_json::json;
use std::fs::File;
use std::io::{BufWriter, Write};
use tempdir::TempDir;

fn main() {
    let json = json!({
        "name": "John Doe",
        "age": 43,
        "address": {
            "street": "10 Downing Street",
            "city": "London"
        },
        "phones": [
            "+44 1234567",
            "+44 2345678"
        ]
    });
    let (_tmpdir, file, filename) = create_tmp_file().unwrap();

    let filename2 = filename.clone();

    let mut writer = BufWriter::new(file);
    serde_json::to_writer(&mut writer, &json).unwrap();
    println!("Writing file {filename2}");
    writer.flush().unwrap();

    let json = query_file(filename).unwrap();
    println!("{json}");
}

fn create_tmp_file() -> Result<(TempDir, File, String), std::io::Error> {
    let tmp_dir = TempDir::new("langdb")?;
    let file_path = tmp_dir.path().join("temp.json");
    let file_path_s = file_path.to_string_lossy().to_string();
    let tmp_file = File::create(file_path)?;
    Ok((tmp_dir, tmp_file, file_path_s))
}

fn query_file(filename: String) -> ChdbResult<String> {
    let q = format!("SELECT * from file(\"{filename}\")");
    let v = query(&q, "JSON", None);

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
