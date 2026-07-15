use std::collections::HashSet;
use std::sync::{Arc, Mutex};

#[cfg(test)]
use tokio::sync::Notify;
use tokio::sync::{mpsc, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::models::IncomingMessage;

use super::{ChatHandler, LongLane, MaxClient, ServeConfig};

pub(super) struct DispatcherRoot {
    shutdown: CancellationToken,
    busy: Mutex<HashSet<i64>>,
    #[cfg(test)]
    freed: Notify,
}

impl DispatcherRoot {
    pub(super) fn new() -> Self {
        Self {
            shutdown: CancellationToken::new(),
            busy: Mutex::new(HashSet::new()),
            #[cfg(test)]
            freed: Notify::new(),
        }
    }

    fn remove_busy(&self, chat_id: i64) {
        self.busy
            .lock()
            .expect("dispatcher busy set poisoned")
            .remove(&chat_id);
        #[cfg(test)]
        self.freed.notify_waiters();
    }

    pub(super) fn abort(&self) {
        self.shutdown.cancel();
    }
}

struct BusyEntry {
    root: Arc<DispatcherRoot>,
    chat_id: i64,
}

impl Drop for BusyEntry {
    fn drop(&mut self) {
        self.root.remove_busy(self.chat_id);
    }
}

#[cfg(test)]
impl DispatcherRoot {
    async fn wait_chat_free(&self, chat_id: i64) {
        loop {
            let notified = self.freed.notified();
            if !self
                .busy
                .lock()
                .expect("dispatcher busy set poisoned")
                .contains(&chat_id)
            {
                return;
            }
            notified.await;
        }
    }
}

pub(super) async fn run<H: ChatHandler>(
    root: Arc<DispatcherRoot>,
    client: MaxClient,
    handler: H,
    config: ServeConfig,
    mut incoming: mpsc::UnboundedReceiver<IncomingMessage>,
) {
    let handler = Arc::new(handler);
    let semaphore = Arc::new(Semaphore::new(config.max_concurrent));
    let lane = LongLane::new(semaphore, root.shutdown.clone());

    loop {
        let message = tokio::select! {
            biased;
            _ = root.shutdown.cancelled() => break,
            message = incoming.recv() => match message {
                Some(message) => message,
                None => break,
            },
        };

        let admitted = {
            let mut busy = root.busy.lock().expect("dispatcher busy set poisoned");
            if busy.contains(&message.chat_id) {
                false
            } else {
                busy.insert(message.chat_id);
                true
            }
        };

        if !admitted {
            tracing::warn!(
                chat_id = message.chat_id,
                message_id = message.message_id,
                "dropping message because its chat is busy"
            );
            continue;
        }

        tokio::spawn(run_handler(
            Arc::clone(&root),
            client.clone(),
            Arc::clone(&handler),
            lane.clone(),
            message,
        ));
    }
}

async fn run_handler<H: ChatHandler>(
    root: Arc<DispatcherRoot>,
    client: MaxClient,
    handler: Arc<H>,
    lane: LongLane,
    message: IncomingMessage,
) {
    let chat_id = message.chat_id;
    let message_id = message.message_id;
    let busy = BusyEntry { root, chat_id };

    tokio::select! {
        biased;
        _ = busy.root.shutdown.cancelled() => {}
        result = handler.on_message(&client, message, &lane) => {
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::Semaphore;

    use crate::auth::LoginConfig;
    use crate::error::{Error, Result};

    use super::*;

    type HandlerFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
    type HandlerCallback =
        dyn Fn(&MaxClient, IncomingMessage, LongLane) -> HandlerFuture + Send + Sync;

    struct TestHandler(Arc<HandlerCallback>);

    impl TestHandler {
        fn new(on_message: Arc<HandlerCallback>) -> Self {
            Self(on_message)
        }
    }

    impl ChatHandler for TestHandler {
        fn on_message(
            &self,
            client: &MaxClient,
            message: IncomingMessage,
            lane: &LongLane,
        ) -> impl Future<Output = Result<()>> + Send {
            let lane = lane.clone();
            (self.0)(client, message, lane)
        }
    }

    fn client() -> MaxClient {
        MaxClient::new(LoginConfig {
            phone: None,
            password: None,
            session_token: None,
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

    fn connected(
        root: Arc<DispatcherRoot>,
        client: MaxClient,
        handler: TestHandler,
        config: ServeConfig,
        incoming: mpsc::UnboundedReceiver<IncomingMessage>,
    ) -> super::super::ConnectedClient<TestHandler> {
        super::super::ConnectedClient {
            client,
            handler,
            config,
            incoming,
            dispatcher: root,
        }
    }

    async fn serve(
        handler: TestHandler,
        max_concurrent: usize,
    ) -> (
        mpsc::UnboundedSender<IncomingMessage>,
        Arc<DispatcherRoot>,
        tokio::task::JoinHandle<()>,
    ) {
        let client = client();
        let root = Arc::new(DispatcherRoot::new());
        let (tx, rx) = mpsc::unbounded_channel();
        let connected = connected(
            Arc::clone(&root),
            client,
            handler,
            ServeConfig { max_concurrent },
            rx,
        );
        let run_task = tokio::spawn(connected.run());
        (tx, root, run_task)
    }

    fn stop(root: &DispatcherRoot) {
        root.abort();
    }

    #[tokio::test]
    async fn blocked_chat_does_not_delay_another_chat() {
        let gate = Arc::new(Semaphore::new(0));
        let handler_gate = Arc::clone(&gate);
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let handler = TestHandler::new(Arc::new(move |_, message, _lane| {
            let gate = Arc::clone(&handler_gate);
            let started_tx = started_tx.clone();
            Box::pin(async move {
                started_tx.send(message.chat_id).unwrap();
                gate.acquire().await.unwrap().forget();
                Ok(())
            })
        }));
        let (tx, root, _run_task) = serve(handler, 2).await;

        tx.send(message(1, 1)).unwrap();
        assert_eq!(started_rx.recv().await, Some(1));
        tx.send(message(2, 2)).unwrap();
        assert_eq!(started_rx.recv().await, Some(2));

        stop(&root);
    }

    #[tokio::test]
    async fn busy_message_is_dropped_and_chat_runs_again_after_completion() {
        let gate = Arc::new(Semaphore::new(0));
        let handler_gate = Arc::clone(&gate);
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let handler = TestHandler::new(Arc::new(move |_, message, _lane| {
            let gate = Arc::clone(&handler_gate);
            let started_tx = started_tx.clone();
            Box::pin(async move {
                started_tx.send(message.message_id).unwrap();
                if message.chat_id == 1 {
                    gate.acquire().await.unwrap().forget();
                }
                Ok(())
            })
        }));
        let (tx, root, _run_task) = serve(handler, 8).await;

        tx.send(message(1, 1)).unwrap();
        assert_eq!(started_rx.recv().await, Some(1));
        tx.send(message(1, 2)).unwrap();
        tx.send(message(2, 99)).unwrap();
        assert_eq!(started_rx.recv().await, Some(99));
        gate.add_permits(1);
        root.wait_chat_free(1).await;

        tx.send(message(1, 3)).unwrap();
        assert_eq!(started_rx.recv().await, Some(3));
        stop(&root);
    }

    #[tokio::test]
    async fn concurrency_never_exceeds_configured_maximum() {
        let gate = Arc::new(Semaphore::new(0));
        let long_running = Arc::new(AtomicUsize::new(0));
        let long_high_water = Arc::new(AtomicUsize::new(0));
        let fast_running = Arc::new(AtomicUsize::new(0));
        let handler_gate = Arc::clone(&gate);
        let handler_long_running = Arc::clone(&long_running);
        let handler_long_high_water = Arc::clone(&long_high_water);
        let handler_fast_running = Arc::clone(&fast_running);
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let handler = TestHandler::new(Arc::new(move |_, message, lane| {
            let gate = Arc::clone(&handler_gate);
            let long_running = Arc::clone(&handler_long_running);
            let long_high_water = Arc::clone(&handler_long_high_water);
            let fast_running = Arc::clone(&handler_fast_running);
            let started_tx = started_tx.clone();
            Box::pin(async move {
                if message.message_id == 0 {
                    let now = fast_running.fetch_add(1, Ordering::SeqCst) + 1;
                    started_tx.send((message.chat_id, now)).unwrap();
                    gate.acquire().await.unwrap().forget();
                    fast_running.fetch_sub(1, Ordering::SeqCst);
                } else {
                    let _permit = lane.enter().await?;
                    let now = long_running.fetch_add(1, Ordering::SeqCst) + 1;
                    long_high_water.fetch_max(now, Ordering::SeqCst);
                    started_tx.send((message.chat_id, now)).unwrap();
                    gate.acquire().await.unwrap().forget();
                    long_running.fetch_sub(1, Ordering::SeqCst);
                }
                Ok(())
            })
        }));
        let observed_long_high_water = Arc::clone(&long_high_water);
        let (tx, root, _run_task) = serve(handler, 2).await;

        // Two long-lane chats plus three fast-lane chats.
        tx.send(message(1, 1)).unwrap();
        tx.send(message(2, 2)).unwrap();
        tx.send(message(3, 0)).unwrap();
        tx.send(message(4, 0)).unwrap();
        tx.send(message(5, 0)).unwrap();

        // Wait for all five handlers to start.
        for _ in 0..5 {
            started_rx.recv().await.unwrap();
        }

        // Fast lane runs unbounded alongside the long lane.
        assert_eq!(fast_running.load(Ordering::SeqCst), 3);
        // Long lane is still capped by max_concurrent.
        assert_eq!(observed_long_high_water.load(Ordering::SeqCst), 2);

        gate.add_permits(5);
        for chat_id in 1..=5 {
            root.wait_chat_free(chat_id).await;
        }
        assert_eq!(observed_long_high_water.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn erroring_handler_releases_permit_and_frees_chat() {
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let handler = TestHandler::new(Arc::new(move |_, message, lane| {
            let started_tx = started_tx.clone();
            Box::pin(async move {
                let _permit = lane.enter().await?;
                started_tx.send(message.message_id).unwrap();
                Err(Error::UnexpectedResponse("expected test error".into()))
            })
        }));
        let (tx, root, _run_task) = serve(handler, 1).await;

        tx.send(message(1, 1)).unwrap();
        assert_eq!(started_rx.recv().await, Some(1));
        root.wait_chat_free(1).await;
        tx.send(message(1, 2)).unwrap();
        assert_eq!(started_rx.recv().await, Some(2));
    }

    #[tokio::test]
    async fn panicking_handler_releases_permit_and_frees_chat() {
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let handler = TestHandler::new(Arc::new(move |_, message, lane| {
            let started_tx = started_tx.clone();
            Box::pin(async move {
                let _permit = lane.enter().await?;
                started_tx.send(message.message_id).unwrap();
                if message.message_id == 1 {
                    panic!("expected test panic");
                }
                Ok(())
            })
        }));
        let (tx, root, _run_task) = serve(handler, 1).await;

        tx.send(message(1, 1)).unwrap();
        assert_eq!(started_rx.recv().await, Some(1));
        root.wait_chat_free(1).await;
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
        let handler = TestHandler::new(Arc::new(move |_, message, _lane| {
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
        let (tx, root, run_task) = serve(handler, 8).await;

        tx.send(message(1, 1)).unwrap();
        assert_eq!(started_rx.recv().await, Some(1));
        drop(tx);
        run_task.await.unwrap();
        gate.add_permits(1);
        finished_rx.await.unwrap();
        root.wait_chat_free(1).await;
    }

    #[tokio::test]
    async fn cancelling_run_stops_admission_without_cancelling_accepted_handler() {
        let gate = Arc::new(Semaphore::new(0));
        let handler_gate = Arc::clone(&gate);
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        let finished_tx = Arc::new(Mutex::new(Some(finished_tx)));
        let handler = TestHandler::new(Arc::new(move |_, _, _lane| {
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
        let (tx, root, run_task) = serve(handler, 8).await;

        tx.send(message(1, 1)).unwrap();
        started_rx.recv().await.unwrap();
        run_task.abort();
        assert!(run_task.await.unwrap_err().is_cancelled());
        assert!(tx.send(message(2, 2)).is_err());

        gate.add_permits(1);
        finished_rx
            .await
            .expect("cancelling run must not abort an accepted handler");
        root.wait_chat_free(1).await;
    }

    #[tokio::test]
    async fn connection_failure_does_not_cancel_accepted_handler() {
        let gate = Arc::new(Semaphore::new(0));
        let handler_gate = Arc::clone(&gate);
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        let finished_tx = Arc::new(Mutex::new(Some(finished_tx)));
        let handler = TestHandler::new(Arc::new(move |_, message, _lane| {
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
        let client = client();
        let root = Arc::new(DispatcherRoot::new());
        let (tx, rx) = mpsc::unbounded_channel();
        *client.inner.msg_tx.lock().await = Some(super::super::DispatcherSender {
            tx: Some(tx.clone()),
            root: Arc::clone(&root),
        });
        let connected = connected(
            Arc::clone(&root),
            client.clone(),
            handler,
            ServeConfig { max_concurrent: 8 },
            rx,
        );
        let run_task = tokio::spawn(connected.run());

        tx.send(message(2, 1)).unwrap();
        assert_eq!(started_rx.recv().await, Some(1));
        drop(tx);
        client.inner.fail().await;
        run_task.await.unwrap();

        gate.add_permits(1);
        finished_rx
            .await
            .expect("connection failure must not abort accepted handler");
        root.wait_chat_free(2).await;
    }

    #[tokio::test]
    async fn disconnect_after_connection_failure_still_aborts_handler() {
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let handler = TestHandler::new(Arc::new(move |_, message, _lane| {
            let started_tx = started_tx.clone();
            Box::pin(async move {
                started_tx.send(message.message_id).unwrap();
                std::future::pending::<()>().await;
                Ok(())
            })
        }));
        let client = client();
        let root = Arc::new(DispatcherRoot::new());
        let (tx, rx) = mpsc::unbounded_channel();
        *client.inner.msg_tx.lock().await = Some(super::super::DispatcherSender {
            tx: Some(tx.clone()),
            root: Arc::clone(&root),
        });
        let connected = connected(
            Arc::clone(&root),
            client.clone(),
            handler,
            ServeConfig { max_concurrent: 8 },
            rx,
        );
        let run_task = tokio::spawn(connected.run());

        tx.send(message(3, 1)).unwrap();
        assert_eq!(started_rx.recv().await, Some(1));
        drop(tx);
        client.inner.fail().await;
        run_task.await.unwrap();

        client.disconnect().await;
        root.wait_chat_free(3).await;
    }

    #[tokio::test]
    async fn disconnect_stops_run_aborts_handler_and_rejects_new_messages() {
        let gate = Arc::new(Semaphore::new(0));
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let handler = TestHandler::new(Arc::new(move |_, message, _lane| {
            let gate = Arc::clone(&gate);
            let started_tx = started_tx.clone();
            Box::pin(async move {
                started_tx.send(message.message_id).unwrap();
                gate.acquire().await.unwrap().forget();
                Ok(())
            })
        }));
        let client = client();
        let root = Arc::new(DispatcherRoot::new());
        let (tx, rx) = mpsc::unbounded_channel();
        *client.inner.msg_tx.lock().await = Some(super::super::DispatcherSender {
            tx: Some(tx.clone()),
            root: Arc::clone(&root),
        });
        let connected = connected(
            Arc::clone(&root),
            client.clone(),
            handler,
            ServeConfig { max_concurrent: 1 },
            rx,
        );
        let run_task = tokio::spawn(connected.run());

        tx.send(message(1, 1)).unwrap();
        assert_eq!(started_rx.recv().await, Some(1));
        client.disconnect().await;
        run_task.await.unwrap();
        root.wait_chat_free(1).await;
        assert!(tx.send(message(2, 2)).is_err());
        assert!(started_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn handler_disconnect_finishes_cleanup_before_aborting_handlers() {
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (finished_tx, mut finished_rx) = tokio::sync::oneshot::channel();
        let finished_tx = Arc::new(Mutex::new(Some(finished_tx)));
        let handler = TestHandler::new(Arc::new(move |client, _, _lane| {
            let client = client.clone();
            let started_tx = started_tx.clone();
            let finished_tx = Arc::clone(&finished_tx);
            Box::pin(async move {
                started_tx.send(()).unwrap();
                client.disconnect().await;
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
        let client = client();
        let root = Arc::new(DispatcherRoot::new());
        let (tx, rx) = mpsc::unbounded_channel();
        *client.inner.msg_tx.lock().await = Some(super::super::DispatcherSender {
            tx: Some(tx.clone()),
            root: Arc::clone(&root),
        });
        let connected = connected(
            Arc::clone(&root),
            client.clone(),
            handler,
            ServeConfig { max_concurrent: 1 },
            rx,
        );
        let state_owner = client.clone();
        let state_guard = state_owner.inner.state.lock().await;
        let run_task = tokio::spawn(connected.run());

        tx.send(message(1, 1)).unwrap();
        started_rx.recv().await.unwrap();
        drop(tx);
        run_task.await.unwrap();
        assert!(matches!(
            finished_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));

        drop(state_guard);
        finished_rx
            .await
            .expect("handler must finish disconnect cleanup before it is aborted");
        root.wait_chat_free(1).await;
    }

    #[tokio::test]
    async fn handler_without_enter_runs_when_lane_is_saturated() {
        let gate = Arc::new(Semaphore::new(0));
        let handler_gate = Arc::clone(&gate);
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let handler = TestHandler::new(Arc::new(move |_, message, lane| {
            let gate = Arc::clone(&handler_gate);
            let started_tx = started_tx.clone();
            Box::pin(async move {
                started_tx.send(message.chat_id).unwrap();
                if message.chat_id != 2 {
                    let _permit = lane.enter().await?;
                }
                gate.acquire().await.unwrap().forget();
                Ok(())
            })
        }));
        let (tx, root, _run_task) = serve(handler, 1).await;

        tx.send(message(1, 1)).unwrap();
        assert_eq!(started_rx.recv().await, Some(1));

        tx.send(message(2, 2)).unwrap();
        assert_eq!(started_rx.recv().await, Some(2));

        gate.add_permits(2);
        root.wait_chat_free(1).await;
        root.wait_chat_free(2).await;
    }

    #[tokio::test]
    async fn long_lane_waits_for_permit_in_fifo_order() {
        let gate = Arc::new(Semaphore::new(0));
        let handler_gate = Arc::clone(&gate);
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let handler = TestHandler::new(Arc::new(move |_, message, lane| {
            let gate = Arc::clone(&handler_gate);
            let started_tx = started_tx.clone();
            Box::pin(async move {
                let _permit = lane.enter().await?;
                started_tx.send(message.chat_id).unwrap();
                gate.acquire().await.unwrap().forget();
                Ok(())
            })
        }));
        let (tx, root, _run_task) = serve(handler, 1).await;

        tx.send(message(1, 1)).unwrap();
        assert_eq!(started_rx.recv().await, Some(1));

        tx.send(message(2, 2)).unwrap();
        tx.send(message(3, 3)).unwrap();
        tokio::task::yield_now().await;

        assert!(started_rx.try_recv().is_err());

        gate.add_permits(1);
        assert_eq!(started_rx.recv().await, Some(2));

        gate.add_permits(1);
        assert_eq!(started_rx.recv().await, Some(3));
        gate.add_permits(1);

        root.wait_chat_free(1).await;
        root.wait_chat_free(2).await;
        root.wait_chat_free(3).await;
    }

    #[tokio::test]
    async fn busy_message_dropped_while_chat_parked_in_enter() {
        let gate = Arc::new(Semaphore::new(0));
        let handler_gate = Arc::clone(&gate);
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let handler = TestHandler::new(Arc::new(move |_, message, lane| {
            let gate = Arc::clone(&handler_gate);
            let started_tx = started_tx.clone();
            Box::pin(async move {
                let _permit = lane.enter().await?;
                started_tx.send(message.message_id).unwrap();
                gate.acquire().await.unwrap().forget();
                Ok(())
            })
        }));
        let (tx, root, _run_task) = serve(handler, 1).await;

        tx.send(message(1, 1)).unwrap();
        assert_eq!(started_rx.recv().await, Some(1));

        tx.send(message(1, 2)).unwrap();
        tx.send(message(2, 3)).unwrap();
        tx.send(message(2, 4)).unwrap();

        gate.add_permits(1);
        assert_eq!(started_rx.recv().await, Some(3));
        gate.add_permits(1);
        root.wait_chat_free(2).await;

        tx.send(message(2, 5)).unwrap();
        assert_eq!(started_rx.recv().await, Some(5));
        gate.add_permits(1);

        root.wait_chat_free(1).await;
        root.wait_chat_free(2).await;
    }

    #[tokio::test]
    async fn shutdown_while_parked_in_long_lane_aborts_cleanly() {
        let gate = Arc::new(Semaphore::new(0));
        let handler_gate = Arc::clone(&gate);
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let handler = TestHandler::new(Arc::new(move |_, message, lane| {
            let gate = Arc::clone(&handler_gate);
            let started_tx = started_tx.clone();
            Box::pin(async move {
                let _permit = lane.enter().await?;
                started_tx.send(message.chat_id).unwrap();
                gate.acquire().await.unwrap().forget();
                Ok(())
            })
        }));
        let (tx, root, run_task) = serve(handler, 1).await;

        tx.send(message(1, 1)).unwrap();
        assert_eq!(started_rx.recv().await, Some(1));

        tx.send(message(2, 2)).unwrap();
        tokio::task::yield_now().await;

        stop(&root);
        run_task.await.unwrap();
        root.wait_chat_free(1).await;
        root.wait_chat_free(2).await;

        assert!(started_rx.try_recv().is_err());
    }
}
