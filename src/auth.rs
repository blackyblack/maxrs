//! Authentication helpers shared by examples and applications.

use std::env;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use crate::captcha::http::{HttpServer, HttpServerConfig};
use crate::captcha::solver::{CaptchaSolver, CaptchaSolverConfig};
use crate::client::MaxClient;
use crate::error::{Error, Result};

pub const ENV_SESSION_TOKEN: &str = "MAX_SESSION_TOKEN";
pub const ENV_SOLVER_URL: &str = "MAX_SOLVER_URL";
pub const ENV_CALLBACK_BIND: &str = "MAX_CALLBACK_BIND";
pub const ENV_CALLBACK_URL_BASE: &str = "MAX_CALLBACK_URL_BASE";

pub const DEFAULT_SOLVER_URL: &str = "http://127.0.0.1:3000";
pub const DEFAULT_CALLBACK_BIND: &str = "127.0.0.1:3002";
pub const DEFAULT_CAPTCHA_CALLBACK_PATH: &str = "/captcha-callback";

/// Returns the saved Max session token from `MAX_SESSION_TOKEN`, when present.
pub fn session_token_from_env() -> Option<String> {
    env_string(ENV_SESSION_TOKEN)
}

/// Runtime configuration for solving auth captchas through `max_captcha_solver`.
#[derive(Debug, Clone)]
pub struct AuthCaptchaConfig {
    /// Base URL of the solver service solve API.
    pub solver_url: Option<String>,
    /// Local address for the built-in callback receiver.
    pub callback_bind: String,
    /// Public base URL that the solver can use to reach the callback receiver.
    pub callback_url_base: Option<String>,
}

impl AuthCaptchaConfig {
    /// Builds configuration from `MAX_SOLVER_URL`, `MAX_CALLBACK_BIND`, and
    /// `MAX_CALLBACK_URL_BASE`.
    pub fn from_env() -> Self {
        Self {
            solver_url: Some(
                env_string(ENV_SOLVER_URL).unwrap_or_else(|| DEFAULT_SOLVER_URL.into()),
            ),
            callback_bind: env_string(ENV_CALLBACK_BIND)
                .unwrap_or_else(|| DEFAULT_CALLBACK_BIND.into()),
            callback_url_base: env_string(ENV_CALLBACK_URL_BASE),
        }
    }

    /// Builds configuration with captcha solving disabled.
    pub fn disabled() -> Self {
        Self {
            solver_url: None,
            callback_bind: DEFAULT_CALLBACK_BIND.into(),
            callback_url_base: None,
        }
    }

    /// Builds the callback URL sent to the solver for a bound callback server.
    pub fn callback_url(&self, callback_addr: SocketAddr) -> String {
        match &self.callback_url_base {
            Some(base) => {
                let base = base.replace("{port}", &callback_addr.port().to_string());
                format!(
                    "{}{}",
                    base.trim_end_matches('/'),
                    DEFAULT_CAPTCHA_CALLBACK_PATH
                )
            }
            None => {
                let callback_addr = normalize_callback_addr(callback_addr);
                format!("http://{callback_addr}{DEFAULT_CAPTCHA_CALLBACK_PATH}")
            }
        }
    }
}

impl Default for AuthCaptchaConfig {
    fn default() -> Self {
        Self {
            solver_url: Some(DEFAULT_SOLVER_URL.into()),
            callback_bind: DEFAULT_CALLBACK_BIND.into(),
            callback_url_base: None,
        }
    }
}

impl MaxClient {
    /// Requests an SMS auth code and solves auth captcha challenges with a
    /// `max_captcha_solver` service when Max requires one.
    ///
    /// The helper starts the built-in `POST /captcha-callback` receiver for the
    /// lifetime of the current process when a captcha challenge is encountered.
    pub async fn request_sms_code_with_auth_captcha(
        &self,
        phone: &str,
        config: &AuthCaptchaConfig,
    ) -> Result<String> {
        match self.request_sms_code(phone).await {
            Ok(token) => Ok(token),
            Err(Error::CaptchaRequired { link }) => {
                let solver_url = config
                    .solver_url
                    .as_ref()
                    .ok_or(Error::CaptchaSolverDisabled)?;
                let server = HttpServer::bind(HttpServerConfig::new(&config.callback_bind)).await?;
                let callback_addr = server.local_addr()?;
                let callback_url = config.callback_url(callback_addr);
                let solver = Arc::new(CaptchaSolver::new(CaptchaSolverConfig::new(
                    solver_url.clone(),
                    callback_url,
                ))?);

                tokio::spawn(server.with_captcha_solver(Arc::clone(&solver)).serve());

                let captcha_token = solver.solve(&link).await?;
                self.request_sms_code_with_captcha_token(phone, &captcha_token)
                    .await
            }
            Err(err) => Err(err),
        }
    }
}

fn env_string(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn normalize_callback_addr(callback_addr: SocketAddr) -> SocketAddr {
    let port = callback_addr.port();

    match callback_addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
        IpAddr::V6(ip) if ip.is_unspecified() => SocketAddr::from((Ipv6Addr::LOCALHOST, port)),
        _ => callback_addr,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn callback_url_uses_bound_addr_by_default() {
        let config = AuthCaptchaConfig::default();
        let addr = "127.0.0.1:3002".parse().unwrap();

        assert_eq!(
            config.callback_url(addr),
            "http://127.0.0.1:3002/captcha-callback"
        );
    }

    #[test]
    fn callback_url_uses_public_base_and_port_placeholder() {
        let config = AuthCaptchaConfig {
            callback_url_base: Some("https://example.test:{port}/max".into()),
            ..AuthCaptchaConfig::default()
        };
        let addr = "127.0.0.1:3002".parse().unwrap();

        assert_eq!(
            config.callback_url(addr),
            "https://example.test:3002/max/captcha-callback"
        );
    }

    #[test]
    fn callback_url_normalizes_unspecified_bind_addresses() {
        let config = AuthCaptchaConfig::default();

        let ipv4_addr = "0.0.0.0:3002".parse().unwrap();
        assert_eq!(
            config.callback_url(ipv4_addr),
            "http://127.0.0.1:3002/captcha-callback"
        );

        let ipv6_addr = "[::]:3002".parse().unwrap();
        assert_eq!(
            config.callback_url(ipv6_addr),
            "http://[::1]:3002/captcha-callback"
        );
    }
}
