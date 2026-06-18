use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use crate::captcha::CaptchaSolver;
use crate::error::{Error, Result};

const DEFAULT_CAPTCHA_CALLBACK_PATH: &str = "/captcha-callback";

#[derive(Debug, Clone)]
pub struct HttpServerConfig {
    pub bind_addr: String,
    pub max_body_size: usize,
    pub captcha_callback_path: String,
}

impl HttpServerConfig {
    pub fn new(bind_addr: impl Into<String>) -> Self {
        Self {
            bind_addr: bind_addr.into(),
            max_body_size: 1024 * 1024,
            captcha_callback_path: DEFAULT_CAPTCHA_CALLBACK_PATH.into(),
        }
    }

    pub fn with_max_body_size(mut self, max_body_size: usize) -> Self {
        self.max_body_size = max_body_size;
        self
    }

    pub fn with_captcha_callback_path(mut self, captcha_callback_path: impl Into<String>) -> Self {
        self.captcha_callback_path = captcha_callback_path.into();
        self
    }
}

pub struct HttpServer {
    listener: TcpListener,
    config: HttpServerConfig,
    captcha_solver: Option<Arc<CaptchaSolver>>,
}

impl HttpServer {
    pub async fn bind(config: HttpServerConfig) -> Result<Self> {
        let listener = TcpListener::bind(&config.bind_addr).await?;
        Ok(Self {
            listener,
            config,
            captcha_solver: None,
        })
    }

    pub fn with_captcha_solver(mut self, captcha_solver: Arc<CaptchaSolver>) -> Self {
        self.captcha_solver = Some(captcha_solver);
        self
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub async fn serve(self) -> Result<()> {
        loop {
            let (stream, _) = self.listener.accept().await?;
            let config = self.config.clone();
            let captcha_solver = self.captcha_solver.clone();
            tokio::spawn(async move {
                let service = service_fn(move |request| {
                    handle_request(request, config.clone(), captcha_solver.clone())
                });
                if let Err(err) = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await
                {
                    tracing::debug!(?err, "http callback connection failed");
                }
            });
        }
    }
}

async fn handle_request(
    request: Request<Incoming>,
    config: HttpServerConfig,
    captcha_solver: Option<Arc<CaptchaSolver>>,
) -> std::result::Result<Response<Full<Bytes>>, Infallible> {
    let response = match (request.method(), request.uri().path(), captcha_solver) {
        (&Method::POST, path, Some(captcha_solver)) if path == config.captcha_callback_path => {
            match read_body(request.into_body(), config.max_body_size).await {
                Ok(body) => match captcha_solver.handle_callback_json(&body).await {
                    Ok(()) => response(StatusCode::NO_CONTENT, Vec::new()),
                    Err(err) => error_response(err),
                },
                Err(err) => error_response(err),
            }
        }
        _ => response(StatusCode::NOT_FOUND, b"Not Found".to_vec()),
    };
    Ok(response)
}

async fn read_body(mut body: Incoming, max_body_size: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|err| {
            Error::UnexpectedResponse(format!("failed reading HTTP request body: {err}"))
        })?;
        if let Some(chunk) = frame.data_ref() {
            if bytes.len() + chunk.len() > max_body_size {
                return Err(Error::UnexpectedResponse("HTTP request too large".into()));
            }
            bytes.extend_from_slice(chunk);
        }
    }
    Ok(bytes)
}

fn error_response(err: Error) -> Response<Full<Bytes>> {
    match err {
        Error::Json(_)
        | Error::UnexpectedResponse(_)
        | Error::UnknownCaptchaChallenge { .. }
        | Error::CaptchaFailed(_) => {
            response(StatusCode::BAD_REQUEST, err.to_string().into_bytes())
        }
        other => response(
            StatusCode::INTERNAL_SERVER_ERROR,
            other.to_string().into_bytes(),
        ),
    }
}

fn response(status: StatusCode, body: Vec<u8>) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Full::new(Bytes::from(body)))
        .expect("valid HTTP response")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::Instant;
    use tokio::sync::oneshot;

    use crate::captcha::{CaptchaSolver, CaptchaSolverConfig};

    #[tokio::test]
    async fn optional_server_forwards_captcha_callbacks() {
        let solver = Arc::new(CaptchaSolver::new(CaptchaSolverConfig::disabled()).unwrap());
        let (tx, rx) = oneshot::channel();
        solver
            .insert_pending_for_test("challenge-1".into(), Instant::now(), tx)
            .await;

        let server = HttpServer::bind(HttpServerConfig::new("127.0.0.1:0"))
            .await
            .unwrap()
            .with_captcha_solver(Arc::clone(&solver));
        let addr = server.local_addr().unwrap();
        let server_task = tokio::spawn(server.serve());

        let response = reqwest::Client::new()
            .post(format!("http://{addr}/captcha-callback"))
            .json(&json!({
                "challengeId": "challenge-1",
                "status": "ok",
                "token": "session",
            }))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);
        let callback = rx.await.unwrap();
        assert_eq!(callback.challenge_id, "challenge-1");
        assert_eq!(callback.token.as_deref(), Some("session"));

        server_task.abort();
        let _ = server_task.await;
    }

    #[tokio::test]
    async fn body_limit_is_enforced() {
        let solver = Arc::new(CaptchaSolver::new(CaptchaSolverConfig::disabled()).unwrap());
        let server = HttpServer::bind(HttpServerConfig::new("127.0.0.1:0").with_max_body_size(8))
            .await
            .unwrap()
            .with_captcha_solver(solver);
        let addr = server.local_addr().unwrap();
        let server_task = tokio::spawn(server.serve());

        let response = reqwest::Client::new()
            .post(format!("http://{addr}/captcha-callback"))
            .body("this body is too large")
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);

        server_task.abort();
        let _ = server_task.await;
    }
}
