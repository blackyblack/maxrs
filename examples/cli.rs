//! Minimal interactive client. See the README for configuration.

use maxrs::auth::{LoginConfig, SESSION_TOKEN_FILE};
use maxrs::client::{ChatHandler, MaxClient};
use maxrs::models::IncomingMessage;

struct PrintHandler;

impl ChatHandler for PrintHandler {
    async fn on_message(&self, msg: IncomingMessage) -> Result<(), maxrs::error::Error> {
        let text = if msg.text.trim().is_empty() {
            "[non-text message]"
        } else {
            msg.text.trim()
        };
        println!("\n<< {text}");
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "maxrs=info".into()),
        )
        .init();

    let login_config = LoginConfig::from_env()?;
    let client = MaxClient::new(login_config)?;
    println!("Session token is stored in {SESSION_TOKEN_FILE} when refreshed.");
    println!("Listening for incoming messages (Ctrl-C to quit)...");

    let run = client.run(PrintHandler);
    tokio::pin!(run);
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        result = &mut run => result?,
    }
    client.disconnect().await;

    Ok(())
}
