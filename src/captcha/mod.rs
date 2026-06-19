//! Captcha solver integration for authentication challenges.
//!
//! Pending challenges are kept in memory and expire after the configured
//! timeout. Applications can either forward callback request bodies to
//! [`CaptchaSolver::handle_callback_json`] or run [`HttpServer`] to receive
//! `POST /captcha-callback` directly.

pub mod http;
mod solver;

pub use http::{HttpServer, HttpServerConfig};
pub use solver::{CaptchaCallback, CaptchaChallenge, CaptchaSolver, CaptchaSolverConfig};
