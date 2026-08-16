//! A scripted HTTP/1.1 server for retry tests: answers each connection with
//! the next scripted response and records every request it saw. Raw TCP so
//! the tests depend on nothing beyond tokio (the SDK's own tests already
//! mock the router this way).

#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// One scripted response.
#[derive(Clone, Debug)]
pub struct Scripted {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl Scripted {
    pub fn ok_json(body: &str) -> Self {
        Self {
            status: 200,
            headers: vec![("content-type".into(), "application/json".into())],
            body: body.to_string(),
        }
    }

    pub fn status(status: u16) -> Self {
        Self {
            status,
            headers: vec![],
            body: String::new(),
        }
    }

    pub fn with_status(mut self, status: u16) -> Self {
        self.status = status;
        self
    }

    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }

    pub fn throttled(retry_after: &str) -> Self {
        Self::status(429)
            .with_header("retry-after", retry_after)
            .with_header("content-type", "application/json")
            .with_body(r#"{"error":"rate_limited","message":"slow down"}"#)
    }

    pub fn with_body(mut self, body: &str) -> Self {
        self.body = body.to_string();
        self
    }
}

/// A recorded request: method, path, and raw header lines (lowercased names).
#[derive(Clone, Debug)]
pub struct Seen {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
}

impl Seen {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n == &name.to_ascii_lowercase())
            .map(|(_, v)| v.as_str())
    }
}

pub struct ScriptedServer {
    pub base_url: String,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl ScriptedServer {
    /// Serve `script` in order; once exhausted, every further request gets a
    /// `599` so a runaway retry loop is visible in the recorded requests.
    pub async fn start(script: Vec<Scripted>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();
        let recorded = seen.clone();
        tokio::spawn(async move {
            let mut script = script.into_iter();
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let response = script.next().unwrap_or(Scripted::status(599));
                let recorded = recorded.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    loop {
                        let n = match socket.read(&mut chunk).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => n,
                        };
                        buf.extend_from_slice(&chunk[..n]);
                        if let Some(end) = find_header_end(&buf) {
                            let head = String::from_utf8_lossy(&buf[..end]).to_string();
                            let content_length = head
                                .lines()
                                .find_map(|l| {
                                    let (n, v) = l.split_once(':')?;
                                    n.eq_ignore_ascii_case("content-length")
                                        .then(|| v.trim().parse::<usize>().ok())
                                        .flatten()
                                })
                                .unwrap_or(0);
                            if buf.len() >= end + 4 + content_length {
                                let mut lines = head.lines();
                                let request_line = lines.next().unwrap_or_default();
                                let mut parts = request_line.split_whitespace();
                                let method = parts.next().unwrap_or_default().to_string();
                                let path = parts.next().unwrap_or_default().to_string();
                                let headers = lines
                                    .filter_map(|l| {
                                        let (n, v) = l.split_once(':')?;
                                        Some((n.trim().to_ascii_lowercase(), v.trim().to_string()))
                                    })
                                    .collect();
                                recorded.lock().unwrap().push(Seen {
                                    method,
                                    path,
                                    headers,
                                });
                                break;
                            }
                        }
                    }
                    let mut head = format!(
                        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n",
                        response.status,
                        reason(response.status),
                        response.body.len()
                    );
                    for (n, v) in &response.headers {
                        head.push_str(&format!("{n}: {v}\r\n"));
                    }
                    head.push_str("\r\n");
                    let _ = socket.write_all(head.as_bytes()).await;
                    let _ = socket.write_all(response.body.as_bytes()).await;
                    let _ = socket.shutdown().await;
                });
            }
        });
        Self {
            base_url: format!("http://{addr}"),
            seen,
        }
    }

    pub fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }

    pub fn request_count(&self) -> usize {
        self.seen.lock().unwrap().len()
    }
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Unknown",
    }
}
