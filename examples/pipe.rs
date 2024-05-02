use chdb_rust::{query, ChdbResult, Error};

use serde_json::json;
use std::ffi::CString;
use std::fs::File;
use std::io::{BufWriter, Read, Write};

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

    let filename = create_fifo().to_string_lossy().to_string();
    let filename2 = filename.clone();
    let handle = std::thread::spawn(move || {
        println!("Opening file");
        let file = open_fifo_file(&filename2).unwrap();

        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, &json).unwrap();
        println!("Writing file {filename2}");
        writer.flush().unwrap();

        let name = CString::new(filename2).unwrap();

        drop(writer);
        unsafe {
            libc::unlink(name.as_ptr());
        }
        println!("Writer Thread: Done");
    });
    handle.join().unwrap();

    // let json = _reader(filename.clone()).unwrap();
    // println!("{json}");

    println!("Reader Thread: 2: Running query : {filename}");

    let json = query_file(filename).unwrap();
    println!("{json}");
    println!("Reader Thread: Done");

    // handle.join().unwrap();
}
fn create_fifo() -> CString {
    let filename = CString::new("/tmp/langdb.fifo").unwrap(); // No as ptr
    unsafe {
        libc::mkfifo(filename.as_ptr(), 0o644); // as_ptr moved here
    }
    filename
}

fn open_fifo_file(filename: &String) -> Result<File, std::io::Error> {
    File::options().write(true).open(&filename)
}
fn _reader(filename: String) -> std::io::Result<String> {
    let mut f = File::open(&filename)?;
    let mut data = vec![];
    f.read_to_end(&mut data)?;

    let s = String::from_utf8(data.to_vec()).unwrap();
    Ok(s)
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
