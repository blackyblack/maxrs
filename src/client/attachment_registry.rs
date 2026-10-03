use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

#[derive(Default)]
pub(super) struct AttachmentRegistry {
    waiters: Arc<Mutex<HashMap<i64, Registration>>>,
}

struct Registration {
    identity: Arc<()>,
    completion: oneshot::Sender<()>,
}

impl AttachmentRegistry {
    pub(super) fn register(&self, file_id: i64) -> Option<AttachmentWaiter> {
        let (tx, rx) = oneshot::channel();
        let identity = Arc::new(());
        let mut waiters = self.waiters.lock().expect("attachment registry");
        if waiters.contains_key(&file_id) {
            return None;
        }
        waiters.insert(
            file_id,
            Registration {
                identity: Arc::clone(&identity),
                completion: tx,
            },
        );

        Some(AttachmentWaiter {
            waiters: Arc::clone(&self.waiters),
            file_id,
            identity,
            rx,
        })
    }

    pub(super) fn complete(&self, file_id: i64) {
        let registration = self
            .waiters
            .lock()
            .expect("attachment registry")
            .remove(&file_id);
        if let Some(registration) = registration {
            let _ = registration.completion.send(());
        }
    }

    #[cfg(test)]
    pub(super) fn waiter_count(&self) -> usize {
        self.waiters.lock().expect("attachment registry").len()
    }
}

pub(super) struct AttachmentWaiter {
    waiters: Arc<Mutex<HashMap<i64, Registration>>>,
    file_id: i64,
    identity: Arc<()>,
    rx: oneshot::Receiver<()>,
}

impl AttachmentWaiter {
    pub(super) async fn wait(mut self) {
        let _ = (&mut self.rx).await;
    }
}

impl Drop for AttachmentWaiter {
    fn drop(&mut self) {
        let mut waiters = self.waiters.lock().expect("attachment registry");
        if waiters
            .get(&self.file_id)
            .is_some_and(|registration| Arc::ptr_eq(&registration.identity, &self.identity))
        {
            waiters.remove(&self.file_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unrelated_completions_cannot_displace_a_registered_file() {
        let registry = AttachmentRegistry::default();
        let waiter = registry.register(42).unwrap();

        for file_id in 100..200 {
            registry.complete(file_id);
        }
        registry.complete(42);

        tokio::time::timeout(std::time::Duration::from_secs(1), waiter.wait())
            .await
            .expect("registered completion must be retained");
    }

    #[test]
    fn dropping_a_waiter_unregisters_it() {
        let registry = AttachmentRegistry::default();
        let waiter = registry.register(42).unwrap();
        assert_eq!(registry.waiter_count(), 1);

        drop(waiter);

        assert_eq!(registry.waiter_count(), 0);
    }

    #[test]
    fn duplicate_active_file_id_is_rejected() {
        let registry = AttachmentRegistry::default();
        let _waiter = registry.register(42).unwrap();

        assert!(registry.register(42).is_none());
        assert_eq!(registry.waiter_count(), 1);
    }

    #[test]
    fn completed_waiter_cannot_unregister_a_reused_file_id() {
        let registry = AttachmentRegistry::default();
        let completed = registry.register(42).unwrap();
        registry.complete(42);
        let replacement = registry.register(42).unwrap();

        drop(completed);

        assert_eq!(registry.waiter_count(), 1);
        drop(replacement);
    }
}
