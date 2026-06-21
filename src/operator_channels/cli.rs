use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::error::Result;

pub(crate) async fn request_sms_code(phone: &str) -> Result<String> {
    let mut stdout = tokio::io::stdout();
    stdout
        .write_all(format!("Enter the SMS code for {phone}: ").as_bytes())
        .await?;
    stdout.flush().await?;

    let mut line = String::new();
    let mut reader = BufReader::new(tokio::io::stdin());
    reader.read_line(&mut line).await?;
    Ok(line)
}
