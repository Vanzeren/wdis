use once_cell;
use papaya::HashMap;
use std::collections::HashSet;
use thiserror::Error;
use std::io::ErrorKind;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use wdis::buffer::buf;
#[derive(Debug)]
struct ClientMessage {
    data: String,
    response_sender: mpsc::Sender<Vec<u8>>,
}

#[derive(Debug, Error)]
enum ServerError {
    #[error("Invalid command")]
    InvalidCommand,
    #[error("Invalid arguments")]
    InvalidArguments,
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("UTF-8 error: {0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),
}

type Result<T> = std::result::Result<T, ServerError>;

const CMD_TYPES: once_cell::sync::Lazy<HashSet<&str>> = once_cell::sync::Lazy::new(|| {
    ["get", "set", "del", "incr", "decr", "mget", "setnx"]
        .iter()
        .cloned()
        .collect()
});

/// Handles client connections and processes incoming commands
async fn producer(
    mut stream: TcpStream,
    sender: mpsc::Sender<ClientMessage>,
    map: Arc<HashMap<String, String>>,
) -> Result<()> {
    let (response_tx, mut response_rx) = mpsc::channel(32);

    loop {
        let mut num_buf = [0; 4];

        if let Err(e) = stream.read_exact(&mut num_buf).await {
            if e.kind() == ErrorKind::UnexpectedEof {
            println!("Client disconnected");
            return Ok(());
            }
            eprintln!("Read error: {}", e);
            return Ok(());
        }

        let mut size_buf = [0; 4];
        let mut cmd = Vec::new();
        let count = u32::from_be_bytes(num_buf) as usize;
        for _ in 0..count {
            if let Err(e) = stream.read_exact(&mut size_buf).await {
                if e.kind() == ErrorKind::UnexpectedEof {
                println!("Client disconnected");
                return Ok(());
                }
                eprintln!("Read error: {}", e);
                return Ok(());
            }
            let size = u32::from_be_bytes(size_buf).try_into().unwrap();
            let mut buf = buf::new(size);

        if let Err(e) = stream.read_exact(&mut buf.data).await {
            eprintln!("Failed to read data: {}", e);
            return Ok(());
        }
            cmd.push(String::from_utf8_lossy(&buf.data).to_string());
        }

        if !CMD_TYPES.contains(&cmd[0].as_str()) {
            println!("Invalid command");
            return Ok(());
        }
        let map = map.clone();
        
        let response = match cmd[0].as_str() {
            "get" => get(cmd[1].as_str(), map.clone()).await,
            "set" => set(cmd[1].as_str(), cmd[2].as_str(), map.clone()).await,
            _ => {
                println!("Invalid number of arguments");
                return Ok(());
            }
        };

        let msg = ClientMessage {
            data: response,
            response_sender: response_tx.clone(),
        };

        if let Err(e) = sender.send(msg).await {
            eprintln!("Failed to send message to consumer: {}", e);
            return Ok(());
        }

        // Wait for response from consumer
        if let Some(response) = response_rx.recv().await {
            let response_len = response.len() as u32;
            let len_bytes = response_len.to_be_bytes();

            if let Err(e) = stream.write_all(&len_bytes).await {
                eprintln!("Failed to send response length: {}", e);
                return Ok(());
            }

            if let Err(e) = stream.write_all(&response).await {
                eprintln!("Failed to send response: {}", e);
                return Ok(());
            }
        }
    }
}

/// Processes messages from producers and sends responses back
async fn consumer(mut receiver: mpsc::Receiver<ClientMessage>) {
    while let Some(msg) = receiver.recv().await {
        // Process message
        let response = format!("Server received: {}", msg.data);
        // Send response back to producer
        if let Err(e) = msg.response_sender.send(response.into_bytes()).await {
            eprintln!("{}", e);
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let map: Arc<HashMap<String, String>> = Arc::new(HashMap::new());

    let (tx, rx) = mpsc::channel(32);

    // Start consumer task
    tokio::spawn(consumer(rx));

    let listener = TcpListener::bind("127.0.0.1:6387").await.unwrap();
    println!("Listening on 127.0.0.1:6387");

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let sender = tx.clone();
        let map = map.clone();
        tokio::spawn(async move {
            producer(stream, sender, map).await.unwrap();
        });
    }
}


/// Get value by key from the data store
async fn get(key: &str, map: Arc<HashMap<String, String>>) -> String {
    let map = map.pin_owned();
    match map.get(key) {
        Some(value) => value.clone(),
        None => "error".to_string()
    }
}

/// Set key-value pair in the data store
async fn set(key: &str, value: &str, map: Arc<HashMap<String, String>>) -> String {
    let map = map.pin_owned();
    match map.insert(key.to_string(), value.to_string()) {
        Some(_) => "already exists".to_string(),
        None => "OK".to_string()
    }
}

/// Delete key from the data store
async fn del(key: &str, map: Arc<HashMap<String, String>>) -> String {
    let map = map.pin_owned();
    match map.remove(key) {
        Some(_) => "OK".to_string(),
        None => "error".to_string()
    }
}
