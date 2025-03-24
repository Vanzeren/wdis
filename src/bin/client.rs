use bytes::{BufMut, BytesMut};
use std::io::ErrorKind;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use wdis::buffer::buf;
use wdis::pipeline::Pipeline;

#[allow(unused_variables)]
#[tokio::main]
async fn main() {
    let stream = TcpStream::connect("127.0.0.1:6387").await.unwrap();
    let (mut reader, writer) = tokio::io::split(stream);

    let mut p = Pipeline::new(writer);
    // p.assign("set wzr 666").await;
    p.assign("get wzr").await;
    p.execute().await.unwrap();
    p.close().await.unwrap();

    loop {
        let mut size_buf = [0; 4];

        let size_buf_cap = match reader.read_exact(&mut size_buf).await {
            Ok(size) => size,
            Err(e) => {
                if e.kind() == ErrorKind::UnexpectedEof {
                    println!("connection disconnected");
                    return;
                } else {
                    eprintln!("{}", e);
                    println!("connection disconnected");
                    return;
                }
            }
        };

        let mut buf = buf::new(u32::from_be_bytes(size_buf).try_into().unwrap());

        reader.read_exact(&mut buf.data).await.unwrap();

        println!("Received: {:?}", String::from_utf8_lossy(&buf.data));
    }

    
}

fn make_request(cmd_str: &str) -> Result<BytesMut, &'static str> {
    use std::collections::HashSet;

    static CMD_TYPES: once_cell::sync::Lazy<HashSet<&str>> = once_cell::sync::Lazy::new(|| {
        ["get", "set", "del", "incr", "decr", "mget", "setnx"]
            .iter()
            .cloned()
            .collect()
    });

    let request: Vec<&str> = cmd_str.split(' ').collect();
    if request.is_empty() {
        return Err("Empty command");
    }

    let cmd = request[0].to_ascii_lowercase();
    if !CMD_TYPES.contains(&cmd.as_str()) {
        return Err("Invalid command");
    }
    
    match cmd.as_str() {
        "get" if request.len() == 2 => Ok(make_buf(request)),
        "set" if request.len() == 3 => Ok(make_buf(request)),
        _ => Err("Invalid number of arguments"),
    }
}

fn make_buf(request: Vec<&str>) -> BytesMut {
    let total_len: usize =  request.iter().map(|s|  4 + s.len()).sum();
    let mut buf = BytesMut::with_capacity(total_len);

    for (i, data) in request.iter().enumerate() {
        let processed_data = if i == 0 {
            data.to_ascii_lowercase()
        } else {
            data.to_string()
        };
        buf.put_u32(processed_data.len() as u32);
        buf.put_slice(processed_data.as_bytes());
    }
    buf
}
