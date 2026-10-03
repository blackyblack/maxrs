use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::models::IncomingMessage;

use super::ChatHandler;

pub(super) async fn run<H: ChatHandler>(
    shutdown: CancellationToken,
    handler: Arc<H>,
    mut incoming: mpsc::UnboundedReceiver<IncomingMessage>,
) {
    loop {
        let message = tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            message = incoming.recv() => match message {
                Some(message) => message,
                None => break,
            },
        };

        // Start every admitted handler promptly so it can acknowledge long work
        // before waiting. The dispatcher cannot infer that acknowledgement point
        // from the handler future; applications should bound only their expensive
        // work after sending the initial response.
        tokio::spawn(run_handler(shutdown.clone(), Arc::clone(&handler), message));
    }
}

async fn run_handler<H: ChatHandler>(
    shutdown: CancellationToken,
    handler: Arc<H>,
    message: IncomingMessage,
) {
    let chat_id = message.chat_id;
    let message_id = message.message_id;

    tokio::select! {
        biased;
        _ = shutdown.cancelled() => {}
        result = handler.on_message(message) => {
            if let Err(err) = result {
                tracing::warn!(chat_id, message_id, %err, "message handler failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;

    use tokio::sync::Semaphore;

    use crate::auth::LoginConfig;
    use crate::client::MaxClient;
    use crate::error::{Error, Result};

    use super::*;

    type HandlerFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
    type HandlerCallback = dyn Fn(IncomingMessage) -> HandlerFuture + Send + Sync;

    struct TestHandler(Arc<HandlerCallback>);

    struct DropSignal(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(signal) = self.0.take() {
                let _ = signal.send(());
            }
        }
    }

    impl TestHandler {
        fn new(on_message: Arc<HandlerCallback>) -> Self {
            Self(on_message)
        }
    }

    impl ChatHandler for TestHandler {
        fn on_message(&self, message: IncomingMessage) -> impl Future<Output = Result<()>> + Send {
            (self.0)(message)
        }
    }

    fn client() -> MaxClient {
        MaxClient::new(LoginConfig {
            phone: None,
            password: None,
            session_token: Some("test-token".into()),
            captcha: crate::auth::AuthCaptchaConfig {
                solver_url: None,
                callback_bind: "127.0.0.1:0".into(),
                callback_url_base: None,
            },
            operator: crate::auth::operator_channels::OperatorChannel::None,
        })
        .expect("test client")
    }

    fn message(chat_id: i64, message_id: i64) -> IncomingMessage {
        IncomingMessage {
            chat_id,
            message_id,
            sender: 7,
            text: format!("message {message_id}"),
            time: 11,
        }
    }

    async fn serve(
        handler: TestHandler,
    ) -> (
        mpsc::UnboundedSender<IncomingMessage>,
        CancellationToken,
        tokio::task::JoinHandle<()>,
    ) {
        let root = CancellationToken::new();
        let (tx, rx) = mpsc::unbounded_channel();
        let run_task = tokio::spawn(run(root.clone(), Arc::new(handler), rx));
        (tx, root, run_task)
    }

    fn stop(root: &CancellationToken) {
        root.cancel();
    }

    #[tokio::test]
    async fn blocked_chat_does_not_delay_another_chat() {
        let gate = Arc::new(Semaphore::new(0));
        let handler_gate = Arc::clone(&gate);
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let handler = TestHandler::new(Arc::new(move |message| {
            let gate = Arc::clone(&handler_gate);
            let started_tx = started_tx.clone();
            Box::pin(async move {
                started_tx.send(message.chat_id).unwrap();
                gate.acquire().await.unwrap().forget();
                Ok(())
            })
        }));
        let (tx, root, _run_task) = serve(handler).await;

        tx.send(message(1, 1)).unwrap();
        assert_eq!(started_rx.recv().await, Some(1));
        tx.send(message(2, 2)).unwrap();
        assert_eq!(started_rx.recv().await, Some(2));

        stop(&root);
    }

    #[tokio::test]
    async fn same_chat_message_is_dispatched_while_previous_is_pending() {
        let gate = Arc::new(Semaphore::new(0));
        let handler_gate = Arc::clone(&gate);
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let handler = TestHandler::new(Arc::new(move |message| {
            let gate = Arc::clone(&handler_gate);
            let started_tx = started_tx.clone();
            Box::pin(async move {
                started_tx.send(message.message_id).unwrap();
                if message.message_id == 1 {
                    gate.acquire().await.unwrap().forget();
                }
                Ok(())
            })
        }));
        let (tx, root, _run_task) = serve(handler).await;

        tx.send(message(1, 1)).unwrap();
        assert_eq!(started_rx.recv().await, Some(1));
        tx.send(message(1, 2)).unwrap();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), started_rx.recv())
                .await
                .expect("new message should be dispatched while the previous handler is pending"),
            Some(2)
        );
        gate.add_permits(1);
        stop(&root);
    }

    #[tokio::test]
    async fn erroring_handler_does_not_stop_dispatch() {
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let handler = TestHandler::new(Arc::new(move |message| {
            let started_tx = started_tx.clone();
            Box::pin(async move {
                started_tx.send(message.message_id).unwrap();
                Err(Error::UnexpectedResponse("expected test error".into()))
            })
        }));
        let (tx, _root, _run_task) = serve(handler).await;

        tx.send(message(1, 1)).unwrap();
        assert_eq!(started_rx.recv().await, Some(1));
        tx.send(message(1, 2)).unwrap();
        assert_eq!(started_rx.recv().await, Some(2));
    }

    #[tokio::test]
    async fn panicking_handler_does_not_stop_dispatch() {
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let handler = TestHandler::new(Arc::new(move |message| {
            let started_tx = started_tx.clone();
            Box::pin(async move {
                started_tx.send(message.message_id).unwrap();
                if message.message_id == 1 {
                    panic!("expected test panic");
                }
                Ok(())
            })
        }));
        let (tx, _root, _run_task) = serve(handler).await;

        tx.send(message(1, 1)).unwrap();
        assert_eq!(started_rx.recv().await, Some(1));
        tx.send(message(1, 2)).unwrap();
        assert_eq!(started_rx.recv().await, Some(2));
    }

    #[tokio::test]
    async fn incoming_feed_closure_does_not_cancel_accepted_handler() {
        let gate = Arc::new(Semaphore::new(0));
        let handler_gate = Arc::clone(&gate);
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        let finished_tx = Arc::new(Mutex::new(Some(finished_tx)));
        let handler = TestHandler::new(Arc::new(move |message| {
            let gate = Arc::clone(&handler_gate);
            let started_tx = started_tx.clone();
            let finished_tx = Arc::clone(&finished_tx);
            Box::pin(async move {
                started_tx.send(message.message_id).unwrap();
                gate.acquire().await.unwrap().forget();
                finished_tx
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap()
                    .send(())
                    .unwrap();
                Ok(())
            })
        }));
        let (tx, _root, run_task) = serve(handler).await;

        tx.send(message(1, 1)).unwrap();
        assert_eq!(started_rx.recv().await, Some(1));
        drop(tx);
        run_task.await.unwrap();
        gate.add_permits(1);
        finished_rx.await.unwrap();
    }

    #[tokio::test]
    async fn cancelling_run_stops_admission_without_cancelling_accepted_handler() {
        let gate = Arc::new(Semaphore::new(0));
        let handler_gate = Arc::clone(&gate);
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        let finished_tx = Arc::new(Mutex::new(Some(finished_tx)));
        let handler = TestHandler::new(Arc::new(move |_| {
            let gate = Arc::clone(&handler_gate);
            let started_tx = started_tx.clone();
            let finished_tx = Arc::clone(&finished_tx);
            Box::pin(async move {
                started_tx.send(()).unwrap();
                gate.acquire().await.unwrap().forget();
                finished_tx
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap()
                    .send(())
                    .unwrap();
                Ok(())
            })
        }));
        let (tx, _root, run_task) = serve(handler).await;

        tx.send(message(1, 1)).unwrap();
        started_rx.recv().await.unwrap();
        run_task.abort();
        assert!(run_task.await.unwrap_err().is_cancelled());
        assert!(tx.send(message(2, 2)).is_err());

        gate.add_permits(1);
        finished_rx
            .await
            .expect("cancelling run must not abort an accepted handler");
    }

    #[tokio::test]
    async fn disconnect_after_connection_failure_still_aborts_handler() {
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (aborted_tx, aborted_rx) = tokio::sync::oneshot::channel();
        let aborted_tx = Arc::new(Mutex::new(Some(aborted_tx)));
        let handler = TestHandler::new(Arc::new(move |message| {
            let started_tx = started_tx.clone();
            let aborted_tx = Arc::clone(&aborted_tx);
            Box::pin(async move {
                let _aborted = DropSignal(aborted_tx.lock().unwrap().take());
                started_tx.send(message.message_id).unwrap();
                std::future::pending::<()>().await;
                Ok(())
            })
        }));
        let client = client();
        let connection = client.inner.recovery.begin_attempt().unwrap();
        client.inner.recovery.connected(&connection).unwrap();
        let root = client.inner.handler_shutdown.clone();
        let (tx, rx) = mpsc::unbounded_channel();
        let run_task = tokio::spawn(run(root.clone(), Arc::new(handler), rx));

        tx.send(message(3, 1)).unwrap();
        assert_eq!(started_rx.recv().await, Some(1));
        drop(tx);
        connection.cancel();
        client.inner.recovery.offline();
        run_task.await.unwrap();

        client.disconnect().await;
        aborted_rx.await.expect("disconnect must abort the handler");
    }

    #[tokio::test]
    async fn disconnect_stops_run_aborts_handler_and_rejects_new_messages() {
        let gate = Arc::new(Semaphore::new(0));
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (aborted_tx, aborted_rx) = tokio::sync::oneshot::channel();
        let aborted_tx = Arc::new(Mutex::new(Some(aborted_tx)));
        let handler = TestHandler::new(Arc::new(move |message| {
            let gate = Arc::clone(&gate);
            let started_tx = started_tx.clone();
            let aborted_tx = Arc::clone(&aborted_tx);
            Box::pin(async move {
                let _aborted = DropSignal(aborted_tx.lock().unwrap().take());
                started_tx.send(message.message_id).unwrap();
                gate.acquire().await.unwrap().forget();
                Ok(())
            })
        }));
        let client = client();
        let root = client.inner.handler_shutdown.clone();
        let (tx, rx) = mpsc::unbounded_channel();
        let run_task = tokio::spawn(run(root.clone(), Arc::new(handler), rx));

        tx.send(message(1, 1)).unwrap();
        assert_eq!(started_rx.recv().await, Some(1));
        client.disconnect().await;
        run_task.await.unwrap();
        aborted_rx.await.expect("disconnect must abort the handler");
        assert!(tx.send(message(2, 2)).is_err());
        assert!(started_rx.try_recv().is_err());
    }
}
