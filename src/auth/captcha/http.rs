use std::convert::Infallible;
use std::error::Error as StdError;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::error::{Error, Result};

use super::solver::CaptchaSolver;

const DEFAULT_CAPTCHA_CALLBACK_PATH: &str = "/captcha-callback";
const DEFAULT_CALLBACK_BODY_LIMIT_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone)]
pub struct HttpServerConfig {
    pub bind_addr: String,
}

impl HttpServerConfig {
    pub fn new(bind_addr: impl Into<String>) -> Self {
        Self {
            bind_addr: bind_addr.into(),
        }
    }
}

pub struct HttpServer {
    listener: TcpListener,
    state: Arc<HttpState>,
}

#[must_use = "dropping the server task stops the HTTP server"]
pub struct HttpServerTask {
    handle: Option<JoinHandle<Result<()>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

#[derive(Clone)]
struct HttpState {
    captcha_solver: Option<Arc<CaptchaSolver>>,
}

impl HttpServer {
    pub async fn bind(config: HttpServerConfig) -> Result<Self> {
        let listener = TcpListener::bind(&config.bind_addr).await?;
        Ok(Self {
            listener,
            state: Arc::new(HttpState {
                captcha_solver: None,
            }),
        })
    }

    pub fn with_captcha_solver(mut self, captcha_solver: Arc<CaptchaSolver>) -> Self {
        Arc::make_mut(&mut self.state).captcha_solver = Some(captcha_solver);
        self
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub async fn serve(self, shutdown: impl Future<Output = ()>) -> Result<()> {
        tokio::pin!(shutdown);
        loop {
            let (stream, _) = tokio::select! {
                result = self.listener.accept() => result?,
                () = &mut shutdown => return Ok(()),
            };
            let state = Arc::clone(&self.state);
            tokio::spawn(async move {
                let service = service_fn(move |request| route_request(request, Arc::clone(&state)));
                if let Err(err) = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await
                {
                    tracing::debug!(?err, "http callback connection failed");
                }
            });
        }
    }

    pub fn spawn(self) -> HttpServerTask {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        HttpServerTask {
            handle: Some(tokio::spawn(self.serve(async move {
                let _ = shutdown_rx.await;
            }))),
            shutdown_tx: Some(shutdown_tx),
        }
    }
}

impl HttpServerTask {
    fn request_shutdown(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            if shutdown_tx.send(()).is_err() {
                tracing::debug!("http callback server shutdown signal receiver already dropped");
            }
        }
    }

    fn log_task_error(result: std::result::Result<Result<()>, tokio::task::JoinError>) {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                tracing::warn!(%err, "http callback server task failed during shutdown");
            }
            Err(err) => {
                tracing::warn!(%err, "http callback server task panicked during shutdown");
            }
        }
    }

    pub async fn shutdown(mut self) {
        self.request_shutdown();
        if let Some(handle) = self.handle.take() {
            Self::log_task_error(handle.await);
        }
    }
}

impl Drop for HttpServerTask {
    fn drop(&mut self) {
        self.request_shutdown();
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

async fn route_request(
    request: Request<Incoming>,
    state: Arc<HttpState>,
) -> std::result::Result<Response<Full<Bytes>>, Infallible> {
    let response = match (request.method(), request.uri().path()) {
        (&Method::POST, DEFAULT_CAPTCHA_CALLBACK_PATH) => {
            handle_captcha_callback(request, state).await
        }
        (_, DEFAULT_CAPTCHA_CALLBACK_PATH) => method_not_allowed("POST"),
        _ => json_response(
            StatusCode::NOT_FOUND,
            serde_json::json!({ "error": "not found" }),
        ),
    };
    Ok(response)
}

async fn handle_captcha_callback(
    request: Request<Incoming>,
    state: Arc<HttpState>,
) -> Response<Full<Bytes>> {
    let Some(captcha_solver) = state.captcha_solver.as_ref() else {
        return json_response(
            StatusCode::NOT_FOUND,
            serde_json::json!({ "error": "captcha solver not configured" }),
        );
    };

    let body = match read_limited_body(request.into_body()).await {
        Ok(body) => body,
        Err(BodyReadError::TooLarge) => {
            return json_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                serde_json::json!({ "error": "request body too large" }),
            );
        }
        Err(BodyReadError::Read(err)) => {
            return error_response(Error::UnexpectedResponse(format!(
                "failed reading HTTP request body: {err}"
            )));
        }
    };

    match captcha_solver.handle_callback_json(body.as_ref()).await {
        Ok(()) => json_response(StatusCode::OK, serde_json::json!({ "ok": true })),
        Err(err) => error_response(err),
    }
}

async fn read_limited_body(body: Incoming) -> std::result::Result<Bytes, BodyReadError> {
    match Limited::new(body, DEFAULT_CALLBACK_BODY_LIMIT_BYTES)
        .collect()
        .await
    {
        Ok(body) => Ok(body.to_bytes()),
        Err(err) if err.downcast_ref::<LengthLimitError>().is_some() => {
            Err(BodyReadError::TooLarge)
        }
        Err(err) => Err(BodyReadError::Read(err)),
    }
}

#[derive(Debug)]
enum BodyReadError {
    TooLarge,
    Read(Box<dyn StdError + Send + Sync>),
}

fn method_not_allowed(allowed_methods: &'static str) -> Response<Full<Bytes>> {
    let mut response = json_response(
        StatusCode::METHOD_NOT_ALLOWED,
        serde_json::json!({ "error": "method not allowed" }),
    );
    response.headers_mut().insert(
        hyper::header::ALLOW,
        hyper::header::HeaderValue::from_static(allowed_methods),
    );
    response
}

fn error_response(err: Error) -> Response<Full<Bytes>> {
    match err {
        Error::Json(_)
        | Error::UnexpectedResponse(_)
        | Error::UnknownCaptchaChallenge { .. }
        | Error::CaptchaFailed(_) => json_response(
            StatusCode::BAD_REQUEST,
            serde_json::json!({ "error": err.to_string() }),
        ),
        other => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({ "error": other.to_string() }),
        ),
    }
}

fn json_response(status: StatusCode, body: serde_json::Value) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(
            serde_json::to_vec(&body).expect("valid JSON response"),
        )))
        .expect("valid HTTP response")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::Instant;

    use crate::auth::captcha::solver::{CaptchaSolver, CaptchaSolverConfig};

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
        let server_task = server.spawn();

        let response = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .post(format!("http://{addr}/captcha-callback"))
            .json(&json!({
                "challengeId": "challenge-1",
                "status": "ok",
                "token": "session",
            }))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "application/json");
        assert_eq!(response.text().await.unwrap(), r#"{"ok":true}"#);
        let callback = rx.await.unwrap();
        assert_eq!(callback.challenge_id, "challenge-1");
        assert_eq!(callback.token.as_deref(), Some("session"));

        server_task.shutdown().await;
    }

    #[tokio::test]
    async fn stopped_server_releases_fixed_port_for_retry() {
        let initial_server = HttpServer::bind(HttpServerConfig::new("127.0.0.1:0"))
            .await
            .unwrap();
        let fixed_addr = initial_server.local_addr().unwrap();
        drop(initial_server);

        for _ in 0..2 {
            let solver = Arc::new(CaptchaSolver::new(CaptchaSolverConfig::disabled()).unwrap());
            let server = HttpServer::bind(HttpServerConfig::new(fixed_addr.to_string()))
                .await
                .unwrap()
                .with_captcha_solver(solver);
            let server_task = server.spawn();
            server_task.shutdown().await;
        }
    }

    #[tokio::test]
    async fn errors_are_returned_as_json() {
        let solver = Arc::new(CaptchaSolver::new(CaptchaSolverConfig::disabled()).unwrap());
        let server = HttpServer::bind(HttpServerConfig::new("127.0.0.1:0"))
            .await
            .unwrap()
            .with_captcha_solver(solver);
        let addr = server.local_addr().unwrap();
        let server_task = server.spawn();

        let response = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .post(format!("http://{addr}/captcha-callback"))
            .json(&json!({
                "challengeId": "missing",
                "status": "ok",
                "token": "session",
            }))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
        assert_eq!(response.headers()["content-type"], "application/json");
        assert_eq!(
            response.text().await.unwrap(),
            r#"{"error":"unknown captcha challenge: missing"}"#
        );

        server_task.shutdown().await;
    }

    #[tokio::test]
    async fn missing_solver_returns_json_error() {
        let server = HttpServer::bind(HttpServerConfig::new("127.0.0.1:0"))
            .await
            .unwrap();
        let addr = server.local_addr().unwrap();
        let server_task = server.spawn();

        let response = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .post(format!("http://{addr}/captcha-callback"))
            .json(&json!({
                "challengeId": "missing",
                "status": "ok",
                "token": "session",
            }))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
        assert_eq!(response.headers()["content-type"], "application/json");
        assert_eq!(
            response.text().await.unwrap(),
            r#"{"error":"captcha solver not configured"}"#
        );

        server_task.shutdown().await;
    }

    #[tokio::test]
    async fn oversized_callback_body_is_rejected() {
        let solver = Arc::new(CaptchaSolver::new(CaptchaSolverConfig::disabled()).unwrap());
        let server = HttpServer::bind(HttpServerConfig::new("127.0.0.1:0"))
            .await
            .unwrap()
            .with_captcha_solver(solver);
        let addr = server.local_addr().unwrap();
        let server_task = server.spawn();

        let response = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .post(format!("http://{addr}/captcha-callback"))
            .body("x".repeat(DEFAULT_CALLBACK_BODY_LIMIT_BYTES + 1))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(response.headers()["content-type"], "application/json");
        assert_eq!(
            response.text().await.unwrap(),
            r#"{"error":"request body too large"}"#
        );

        server_task.shutdown().await;
    }
}
