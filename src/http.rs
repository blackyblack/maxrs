use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::error::{Error, Result};

type RouteHandler = Arc<
    dyn Fn(HttpRequest) -> Pin<Box<dyn Future<Output = Result<HttpResponse>> + Send>> + Send + Sync,
>;

#[derive(Debug, Clone)]
pub struct HttpServerConfig {
    pub bind_addr: String,
    pub max_body_size: usize,
}

impl HttpServerConfig {
    pub fn new(bind_addr: impl Into<String>) -> Self {
        Self {
            bind_addr: bind_addr.into(),
            max_body_size: 1024 * 1024,
        }
    }

    pub fn with_max_body_size(mut self, max_body_size: usize) -> Self {
        self.max_body_size = max_body_size;
        self
    }
}

#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: String,
    pub path: String,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct HttpResponse {
    status_code: u16,
    body: Vec<u8>,
    content_type: String,
}

impl HttpResponse {
    pub fn new(status_code: u16, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status_code,
            body: body.into(),
            content_type: "text/plain; charset=utf-8".into(),
        }
    }

    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = content_type.into();
        self
    }

    pub fn no_content() -> Self {
        Self {
            status_code: 204,
            body: Vec::new(),
            content_type: "text/plain; charset=utf-8".into(),
        }
    }
}

pub struct HttpServer {
    listener: TcpListener,
    config: HttpServerConfig,
    routes: HashMap<(String, String), RouteHandler>,
}

impl HttpServer {
    pub async fn bind(config: HttpServerConfig) -> Result<Self> {
        let listener = TcpListener::bind(&config.bind_addr).await?;
        Ok(Self {
            listener,
            config,
            routes: HashMap::new(),
        })
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub fn route<F, Fut>(&mut self, method: impl Into<String>, path: impl Into<String>, handler: F)
    where
        F: Fn(HttpRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<HttpResponse>> + Send + 'static,
    {
        self.routes.insert(
            (method.into().to_ascii_uppercase(), path.into()),
            Arc::new(move |request| Box::pin(handler(request))),
        );
    }

    pub async fn serve(self) -> Result<()> {
        let routes = Arc::new(self.routes);
        loop {
            let (mut stream, _) = self.listener.accept().await?;
            let routes = Arc::clone(&routes);
            let max_body_size = self.config.max_body_size;
            tokio::spawn(async move {
                let response = match read_request(&mut stream, max_body_size).await {
                    Ok(request) => {
                        match routes.get(&(request.method.clone(), request.path.clone())) {
                            Some(handler) => match handler(request).await {
                                Ok(response) => response,
                                Err(err) => HttpResponse::new(500, err.to_string()),
                            },
                            None => HttpResponse::new(404, "Not Found"),
                        }
                    }
                    Err(err) => HttpResponse::new(400, err.to_string()),
                };

                let _ = write_response(&mut stream, response).await;
            });
        }
    }
}

async fn read_request(
    stream: &mut tokio::net::TcpStream,
    max_body_size: usize,
) -> Result<HttpRequest> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 4096];
    let mut header_end = None;

    while header_end.is_none() {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err(Error::UnexpectedResponse(
                "unexpected EOF while reading HTTP request".into(),
            ));
        }
        buffer.extend_from_slice(&chunk[..read]);
        if buffer.len() > max_body_size {
            return Err(Error::UnexpectedResponse("HTTP request too large".into()));
        }
        header_end = find_header_end(&buffer);
    }

    let header_end = header_end.unwrap();
    let header_text = std::str::from_utf8(&buffer[..header_end])
        .map_err(|_| Error::UnexpectedResponse("invalid HTTP header encoding".into()))?;
    let mut lines = header_text.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| Error::UnexpectedResponse("missing HTTP request line".into()))?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .ok_or_else(|| Error::UnexpectedResponse("missing HTTP method".into()))?
        .to_ascii_uppercase();
    let path = request_parts
        .next()
        .ok_or_else(|| Error::UnexpectedResponse("missing HTTP path".into()))?
        .to_string();
    let mut headers = HashMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| Error::UnexpectedResponse("invalid HTTP header".into()))?;
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }

    let content_length = headers
        .get("content-length")
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(|_| Error::UnexpectedResponse("invalid Content-Length".into()))?
        .unwrap_or(0);
    if content_length > max_body_size {
        return Err(Error::UnexpectedResponse("HTTP request too large".into()));
    }

    let body_start = header_end + 4;
    while buffer.len() < body_start + content_length {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err(Error::UnexpectedResponse(
                "unexpected EOF while reading HTTP body".into(),
            ));
        }
        buffer.extend_from_slice(&chunk[..read]);
        if buffer.len() > body_start + content_length {
            buffer.truncate(body_start + content_length);
        }
    }

    Ok(HttpRequest {
        method,
        path,
        headers,
        body: buffer[body_start..body_start + content_length].to_vec(),
    })
}

async fn write_response(stream: &mut tokio::net::TcpStream, response: HttpResponse) -> Result<()> {
    let status_text = match response.status_code {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "OK",
    };
    let headers = format!(
        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nContent-Type: {}\r\nConnection: close\r\n\r\n",
        response.status_code,
        status_text,
        response.body.len(),
        response.content_type,
    );
    stream.write_all(headers.as_bytes()).await?;
    stream.write_all(&response.body).await?;
    stream.shutdown().await?;
    Ok(())
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}
