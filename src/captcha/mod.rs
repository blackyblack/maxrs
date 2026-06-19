//! Captcha solver integration for authentication challenges.
//!
//! Pending challenges are kept in memory and expire after the configured
//! timeout. Applications can either forward callback request bodies to
//! [`solver::CaptchaSolver::handle_callback_json`] or run
//! [`http::HttpServer`] to receive `POST /captcha-callback` directly.

pub mod http;
pub mod solver;
