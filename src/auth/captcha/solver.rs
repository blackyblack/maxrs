use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, Mutex};
use uuid::Uuid;

use crate::error::{Error, Result};

const DEFAULT_CHALLENGE_TIMEOUT: Duration = Duration::from_secs(60 * 60);

/// Configuration for the optional captcha solver integration.
#[derive(Debug, Clone, Default)]
pub struct CaptchaSolverConfig {
    /// Base URL of the solver service, for example `https://solver.example`.
    /// When unset, starting solver-backed challenges is disabled.
    pub solver_url: Option<String>,
    /// Public callback URL that the solver should `POST` to when the challenge
    /// is solved. If unset, the field is omitted from `/solve` requests.
    pub callback_url: Option<String>,
}

impl CaptchaSolverConfig {
    /// Builds enabled solver configuration.
    pub fn new(solver_url: impl Into<String>, callback_url: impl Into<String>) -> Self {
        Self {
            solver_url: Some(solver_url.into()),
            callback_url: Some(callback_url.into()),
        }
    }

    /// Builds disabled configuration.
    pub fn disabled() -> Self {
        Self::default()
    }
}

/// Accepted solver request metadata.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
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
        Ok(Self {
            config,
            http: reqwest::Client::new(),
            pending: Mutex::new(HashMap::new()),
        })
    }

    /// Returns true when a solver service URL is configured.
    pub fn is_enabled(&self) -> bool {
        self.config.solver_url.is_some()
    }

    /// Starts solving `captcha_url` and waits up to one hour for its callback.
    pub async fn solve(&self, captcha_url: &str) -> Result<String> {
        self.cleanup_expired().await;

        let solver_url = self
            .config
            .solver_url
            .as_ref()
            .ok_or(Error::CaptchaSolverDisabled)?;
        let challenge_id = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        let challenge_started_at = Instant::now();
        self.pending.lock().await.insert(
            challenge_id.clone(),
            PendingChallenge {
                created_at: challenge_started_at,
                tx,
            },
        );

        let request = SolveRequest {
            challenge_id: challenge_id.clone(),
            captcha_url: captcha_url.to_string(),
            callback_url: self.config.callback_url.clone(),
        };
        let result = match tokio::time::timeout(
            DEFAULT_CHALLENGE_TIMEOUT,
            self.http
                .post(format!("{}/solve", solver_url.trim_end_matches('/')))
                .json(&request)
                .send(),
        )
        .await
        {
            Ok(result) => result.and_then(|response| response.error_for_status()),
            Err(_) => {
                self.pending.lock().await.remove(&challenge_id);
                return Err(Error::CaptchaTimeout { challenge_id });
            }
        };

        if let Err(err) = result {
            self.pending.lock().await.remove(&challenge_id);
            return Err(Error::CaptchaSolverUnavailable {
                solver_url: solver_url.clone(),
                source: err,
            });
        }

        let remaining = DEFAULT_CHALLENGE_TIMEOUT.saturating_sub(challenge_started_at.elapsed());
        let callback = match tokio::time::timeout(remaining, rx).await {
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

    /// Handles a solver callback JSON body.
    pub async fn handle_callback_json(&self, body: &[u8]) -> Result<()> {
        let callback: CaptchaCallback = serde_json::from_slice(body)?;
        self.handle_callback(callback).await
    }

    /// Handles an already decoded solver callback payload.
    async fn handle_callback(&self, callback: CaptchaCallback) -> Result<()> {
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
    async fn cleanup_expired(&self) {
        self.pending
            .lock()
            .await
            .retain(|_, pending| pending.created_at.elapsed() < DEFAULT_CHALLENGE_TIMEOUT);
    }

    #[cfg(test)]
    pub(crate) async fn insert_pending_for_test(
        &self,
        challenge_id: String,
        created_at: Instant,
        tx: oneshot::Sender<CaptchaCallback>,
    ) {
        self.pending
            .lock()
            .await
            .insert(challenge_id, PendingChallenge { created_at, tx });
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SolveRequest {
    challenge_id: String,
    captcha_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    callback_url: Option<String>,
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
    fn solve_request_serialization() {
        let with_callback = SolveRequest {
            challenge_id: "id-1".into(),
            captcha_url: "https://id.vk.ru/not_robot_captcha".into(),
            callback_url: Some("https://max-login.example/captcha-callback".into()),
        };

        assert_eq!(
            serde_json::to_value(with_callback).unwrap(),
            json!({
                "challengeId": "id-1",
                "captchaUrl": "https://id.vk.ru/not_robot_captcha",
                "callbackUrl": "https://max-login.example/captcha-callback",
            })
        );
        let without_callback = SolveRequest {
            challenge_id: "id-1".into(),
            captcha_url: "https://id.vk.ru/not_robot_captcha".into(),
            callback_url: None,
        };

        assert_eq!(
            serde_json::to_value(without_callback).unwrap(),
            json!({
                "challengeId": "id-1",
                "captchaUrl": "https://id.vk.ru/not_robot_captcha",
            })
        );
    }

    #[test]
    fn captcha_challenge_deserializes_solver_payload() {
        let challenge: CaptchaChallenge = serde_json::from_value(json!({
            "challengeId": "id-1",
            "status": "pending",
            "operatorUrl": "https://solver.example/operator",
        }))
        .unwrap();

        assert_eq!(challenge.challenge_id, "id-1");
        assert_eq!(challenge.status, "pending");
        assert_eq!(
            challenge.operator_url.as_deref(),
            Some("https://solver.example/operator")
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
