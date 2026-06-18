//! Captcha solver integration for authentication challenges.
//!
//! The module is intentionally transport-agnostic for inbound callbacks: wire
//! [`CaptchaSolver::handle_callback_json`] into any HTTP `POST` route exposed at
//! [`CaptchaSolverConfig::callback_url`]. Pending challenges are kept in memory
//! and expire after the configured timeout (one hour by default).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, Mutex};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::models::BROWSER_USER_AGENT;

const DEFAULT_CHALLENGE_TIMEOUT: Duration = Duration::from_secs(60 * 60);

/// Configuration for the optional captcha solver integration.
#[derive(Debug, Clone)]
pub struct CaptchaSolverConfig {
    /// Base URL of the solver service, for example `https://solver.example`.
    /// When unset, starting solver-backed challenges is disabled.
    pub solver_url: Option<String>,
    /// Public callback URL that the solver should `POST` to when the challenge
    /// is solved. If unset, the field is omitted from `/solve` requests.
    pub callback_url: Option<String>,
    /// How long unfinished in-memory challenges may wait for a callback.
    pub challenge_timeout: Duration,
}

impl CaptchaSolverConfig {
    /// Builds enabled solver configuration with the default one-hour timeout.
    pub fn new(solver_url: impl Into<String>, callback_url: impl Into<String>) -> Self {
        Self {
            solver_url: Some(solver_url.into()),
            callback_url: Some(callback_url.into()),
            challenge_timeout: DEFAULT_CHALLENGE_TIMEOUT,
        }
    }

    /// Builds disabled configuration. Useful for making captcha support
    /// explicitly optional in applications.
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Overrides the unfinished challenge timeout.
    pub fn with_challenge_timeout(mut self, timeout: Duration) -> Self {
        self.challenge_timeout = timeout;
        self
    }
}

impl Default for CaptchaSolverConfig {
    fn default() -> Self {
        Self {
            solver_url: None,
            callback_url: None,
            challenge_timeout: DEFAULT_CHALLENGE_TIMEOUT,
        }
    }
}

/// Accepted solver request metadata.
#[derive(Debug, Clone, Deserialize)]
pub struct CaptchaChallenge {
    pub challenge_id: String,
    pub status: String,
    pub operator_url: Option<String>,
}

/// In-memory captcha solver client and callback registry.
pub struct CaptchaSolver {
    config: CaptchaSolverConfig,
    http: reqwest::Client,
    pending: Mutex<HashMap<String, PendingChallenge>>,
}

struct PendingChallenge {
    created_at: Instant,
    tx: oneshot::Sender<CaptchaCallback>,
}

impl CaptchaSolver {
    /// Creates a solver integration from configuration.
    pub fn new(config: CaptchaSolverConfig) -> Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent(BROWSER_USER_AGENT)
            .build()?;
        Ok(Self {
            config,
            http,
            pending: Mutex::new(HashMap::new()),
        })
    }

    /// Returns true when a solver service URL is configured.
    pub fn is_enabled(&self) -> bool {
        self.config.solver_url.is_some()
    }

    /// Starts solving `captcha_url` and waits for the callback token.
    ///
    /// The `/solve` request returns immediately, while this method waits up to
    /// [`CaptchaSolverConfig::challenge_timeout`] for
    /// [`CaptchaSolver::handle_callback_json`] to receive an `ok` or `failed`
    /// callback for the generated challenge id.
    pub async fn solve(&self, captcha_url: &str) -> Result<String> {
        self.cleanup_expired().await;

        let solver_url = self
            .config
            .solver_url
            .as_ref()
            .ok_or(Error::CaptchaSolverDisabled)?;
        let challenge_id = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(
            challenge_id.clone(),
            PendingChallenge {
                created_at: Instant::now(),
                tx,
            },
        );

        let request = SolveRequest {
            challenge_id: &challenge_id,
            captcha_url,
            callback_url: self.config.callback_url.as_deref(),
        };
        let result = self
            .http
            .post(format!("{}/solve", solver_url.trim_end_matches('/')))
            .json(&request)
            .send()
            .await
            .and_then(|response| response.error_for_status());

        if let Err(err) = result {
            self.pending.lock().await.remove(&challenge_id);
            return Err(err.into());
        }

        let callback = match tokio::time::timeout(self.config.challenge_timeout, rx).await {
            Ok(Ok(callback)) => callback,
            Ok(Err(_)) => return Err(Error::CaptchaFailed("callback waiter dropped".into())),
            Err(_) => {
                self.pending.lock().await.remove(&challenge_id);
                return Err(Error::CaptchaTimeout { challenge_id });
            }
        };

        match callback.status.as_str() {
            "ok" => callback
                .token
                .ok_or_else(|| Error::CaptchaFailed("solver returned ok without token".into())),
            "failed" => Err(Error::CaptchaFailed(
                callback.error.unwrap_or_else(|| "solver failed".into()),
            )),
            other => Err(Error::CaptchaFailed(format!(
                "unknown callback status: {other}"
            ))),
        }
    }

    /// Handles a solver callback `POST` body.
    ///
    /// Applications should expose an HTTP route at the configured callback URL
    /// and pass the JSON request body to this method.
    pub async fn handle_callback_json(&self, body: &[u8]) -> Result<()> {
        let callback: CaptchaCallback = serde_json::from_slice(body)?;
        self.handle_callback(callback).await
    }

    /// Handles an already decoded solver callback payload.
    pub async fn handle_callback(&self, callback: CaptchaCallback) -> Result<()> {
        self.cleanup_expired().await;
        let challenge_id = callback.challenge_id.clone();
        let pending = self.pending.lock().await.remove(&challenge_id);
        match pending {
            Some(pending) => pending
                .tx
                .send(callback)
                .map_err(|_| Error::CaptchaFailed("callback receiver dropped".into())),
            None => Err(Error::UnknownCaptchaChallenge { challenge_id }),
        }
    }

    /// Removes expired unfinished challenges from memory.
    pub async fn cleanup_expired(&self) {
        let timeout = self.config.challenge_timeout;
        self.pending
            .lock()
            .await
            .retain(|_, pending| pending.created_at.elapsed() < timeout);
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SolveRequest<'a> {
    challenge_id: &'a str,
    captcha_url: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    callback_url: Option<&'a str>,
}

/// Payload posted by `max_captcha_solver` to the configured callback URL.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CaptchaCallback {
    pub challenge_id: String,
    pub status: String,
    pub token: Option<String>,
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn solve_request_serializes_solver_payload() {
        let request = SolveRequest {
            challenge_id: "id-1",
            captcha_url: "https://id.vk.ru/not_robot_captcha",
            callback_url: Some("https://max-login.example/captcha-callback"),
        };

        let value = serde_json::to_value(request).unwrap();
        assert_eq!(
            value,
            json!({
                "challengeId": "id-1",
                "captchaUrl": "https://id.vk.ru/not_robot_captcha",
                "callbackUrl": "https://max-login.example/captcha-callback",
            })
        );
    }

    #[test]
    fn solve_request_omits_optional_callback_url() {
        let request = SolveRequest {
            challenge_id: "id-1",
            captcha_url: "https://id.vk.ru/not_robot_captcha",
            callback_url: None,
        };

        let value = serde_json::to_value(request).unwrap();
        assert_eq!(
            value,
            json!({
                "challengeId": "id-1",
                "captchaUrl": "https://id.vk.ru/not_robot_captcha",
            })
        );
    }

    #[tokio::test]
    async fn callback_for_unknown_challenge_is_rejected() {
        let solver = CaptchaSolver::new(CaptchaSolverConfig::disabled()).unwrap();
        let err = solver
            .handle_callback_json(br#"{"challengeId":"missing","status":"ok","token":"session"}"#)
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            Error::UnknownCaptchaChallenge { challenge_id } if challenge_id == "missing"
        ));
    }
}
