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
    captcha_solver: Option<Arc<CaptchaSolver>>,
}

impl HttpServer {
    pub async fn bind(config: HttpServerConfig) -> Result<Self> {
        let listener = TcpListener::bind(&config.bind_addr).await?;
        Ok(Self {
            listener,
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
            let captcha_solver = self.captcha_solver.clone();
            tokio::spawn(async move {
                let service =
                    service_fn(move |request| handle_request(request, captcha_solver.clone()));
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
    captcha_solver: Option<Arc<CaptchaSolver>>,
) -> std::result::Result<Response<Full<Bytes>>, Infallible> {
    let response = match (request.method(), request.uri().path()) {
        (&Method::POST, DEFAULT_CAPTCHA_CALLBACK_PATH) => match captcha_solver {
            Some(captcha_solver) => match request.into_body().collect().await {
                Ok(body) => match captcha_solver.handle_callback_json(&body.to_bytes()).await {
                    Ok(()) => json_response(StatusCode::OK, serde_json::json!({ "ok": true })),
                    Err(err) => error_response(err),
                },
                Err(err) => error_response(Error::UnexpectedResponse(format!(
                    "failed reading HTTP request body: {err}"
                ))),
            },
            None => json_response(
                StatusCode::NOT_FOUND,
                serde_json::json!({ "error": "captcha solver not configured" }),
            ),
        },
        (_, DEFAULT_CAPTCHA_CALLBACK_PATH) => Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .header("allow", "POST")
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(
                serde_json::to_vec(&serde_json::json!({ "error": "method not allowed" }))
                    .expect("valid JSON response"),
            )))
            .expect("valid HTTP response"),
        _ => json_response(
            StatusCode::NOT_FOUND,
            serde_json::json!({ "error": "not found" }),
        ),
    };
    Ok(response)
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

        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "application/json");
        assert_eq!(response.text().await.unwrap(), r#"{"ok":true}"#);
        let callback = rx.await.unwrap();
        assert_eq!(callback.challenge_id, "challenge-1");
        assert_eq!(callback.token.as_deref(), Some("session"));

        server_task.abort();
        let _ = server_task.await;
    }

    #[tokio::test]
    async fn errors_are_returned_as_json() {
        let solver = Arc::new(CaptchaSolver::new(CaptchaSolverConfig::disabled()).unwrap());
        let server = HttpServer::bind(HttpServerConfig::new("127.0.0.1:0"))
            .await
            .unwrap()
            .with_captcha_solver(solver);
        let addr = server.local_addr().unwrap();
        let server_task = tokio::spawn(server.serve());

        let response = reqwest::Client::new()
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

        server_task.abort();
        let _ = server_task.await;
    }

    #[tokio::test]
    async fn missing_solver_reports_json_error() {
        let server = HttpServer::bind(HttpServerConfig::new("127.0.0.1:0"))
            .await
            .unwrap();
        let addr = server.local_addr().unwrap();
        let server_task = tokio::spawn(server.serve());

        let response = reqwest::Client::new()
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

        server_task.abort();
        let _ = server_task.await;
    }
}
