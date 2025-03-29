use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncWriteExt, WriteHalf};
use tokio::net::TcpStream;
/// Pipeline for buffering and writing data to a TCP stream
pub struct Pipeline {
    writer: WriteHalf<TcpStream>,
    buf: BytesMut,
}

impl Pipeline {
    /// Create a new Pipeline with the given TCP stream
    pub fn new(writer: WriteHalf<TcpStream>) -> Self {
        Self {
            writer,
            buf: BytesMut::with_capacity(1024),
        }
    }

    /// Add data to the pipeline buffer
    pub async fn assign(&mut self, data: &str) {
        let request = make_request(data).unwrap();

        self.buf.put_slice(&request);
    }

    /// Write buffered data to the stream
    pub async fn execute(&mut self) -> std::io::Result<()> {
        self.writer.write_all(&self.buf).await?;
        self.buf.clear();
        Ok(())
    }

    /// Close the connection gracefully
    pub async fn close(&mut self) -> std::io::Result<()> {
        self.writer.shutdown().await?;
        println!("Connection closed gracefully");
        Ok(())
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
    let request_len = request.len() as u32;
    let total_len: usize = request.iter().map(|s| 4 + s.len()).sum();
    let mut buf = BytesMut::with_capacity(total_len + 4);
    buf.put_u32(request_len);
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
