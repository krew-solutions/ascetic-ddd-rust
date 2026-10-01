//! A subscription cancelled is a handler that is not running and will not
//! be run: `cancel` waits for the call in flight, from inside a call it
//! returns, and a loop that serves a subscription is waited for to return.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use ascetic_ddd_bus::adapters::in_memory::InMemoryBroker;
use ascetic_ddd_bus::handling;
use ascetic_ddd_bus::{Bus, Message};
use tokio::sync::Notify;
use tokio::time::timeout;

const SOON: Duration = Duration::from_millis(200);

#[tokio::test(flavor = "current_thread")]
async fn a_cancel_waits_for_the_handler_in_flight_and_no_call_follows() {
    let mut bus = Bus::new();
    bus.register("in-memory", InMemoryBroker::new()).unwrap();
    let (started, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let subscription = bus
        .consumer("in-memory://orders", "billing", |m: &Message| Ok(m.clone()))
        .unwrap()
        .subscribe({
            let (started, release, calls) = (
                Arc::clone(&started),
                Arc::clone(&release),
                Arc::clone(&calls),
            );
            move |_: Message| {
                let (started, release, calls) = (
                    Arc::clone(&started),
                    Arc::clone(&release),
                    Arc::clone(&calls),
                );
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    started.notify_one();
                    release.notified().await;
                }
            }
        })
        .unwrap();
    let producer = bus
        .producer("in-memory://orders", |m: &Message| m.clone())
        .unwrap();
    producer
        .publish(&Message::new(b"one".to_vec()))
        .await
        .unwrap();
    started.notified().await;

    let cancel = subscription.cancel();
    tokio::pin!(cancel);
    assert!(
        timeout(SOON, &mut cancel).await.is_err(),
        "the cancel waits while the handler runs"
    );
    release.notify_one();
    timeout(SOON, &mut cancel)
        .await
        .expect("the cancel returns once the handler has");

    producer
        .publish(&Message::new(b"two".to_vec()))
        .await
        .unwrap();
    tokio::time::sleep(SOON).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1, "no call after the cancel");
}

#[tokio::test(flavor = "current_thread")]
async fn cancelling_an_earlier_subscription_of_a_group_leaves_the_later_one() {
    let mut bus = Bus::new();
    bus.register("in-memory", InMemoryBroker::new()).unwrap();
    let consumer = bus
        .consumer("in-memory://orders", "billing", |m: &Message| Ok(m.clone()))
        .unwrap();
    let (seen, mut later) = tokio::sync::mpsc::unbounded_channel();
    let earlier = consumer.subscribe(|_: Message| async {}).unwrap();
    let _later = consumer
        .subscribe(move |m: Message| {
            let seen = seen.clone();
            async move {
                seen.send(m).ok();
            }
        })
        .unwrap();
    earlier.cancel().await;
    bus.producer("in-memory://orders", |m: &Message| m.clone())
        .unwrap()
        .publish(&Message::new(b"still delivered".to_vec()))
        .await
        .unwrap();
    assert_eq!(
        timeout(SOON, later.recv())
            .await
            .unwrap()
            .unwrap()
            .payload(),
        b"still delivered"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_handler_may_cancel_its_own_subscription() {
    let mut bus = Bus::new();
    bus.register("in-memory", InMemoryBroker::new()).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let slot: Arc<std::sync::Mutex<Option<Arc<ascetic_ddd_bus::Subscription>>>> =
        Default::default();
    let subscription = bus
        .consumer("in-memory://orders", "billing", |m: &Message| Ok(m.clone()))
        .unwrap()
        .subscribe({
            let (calls, slot) = (Arc::clone(&calls), Arc::clone(&slot));
            move |_: Message| {
                let (calls, slot) = (Arc::clone(&calls), Arc::clone(&slot));
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let mine = slot.lock().unwrap().clone().unwrap();
                    mine.cancel().await; // returns: a handler does not wait for itself
                }
            }
        })
        .unwrap();
    let subscription = Arc::new(subscription);
    *slot.lock().unwrap() = Some(Arc::clone(&subscription));
    let producer = bus
        .producer("in-memory://orders", |m: &Message| m.clone())
        .unwrap();
    producer
        .publish(&Message::new(b"last".to_vec()))
        .await
        .unwrap();
    producer
        .publish(&Message::new(b"never".to_vec()))
        .await
        .unwrap();
    tokio::time::sleep(SOON).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the message in hand was the last"
    );
    timeout(SOON, subscription.cancel())
        .await
        .expect("a cancel from outside finds nothing in flight");
}

#[tokio::test(flavor = "current_thread")]
async fn a_served_loop_is_waited_for_to_return() {
    let stop = Arc::new(Notify::new());
    let (started, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let returned = Arc::new(AtomicUsize::new(0));
    let subscription = handling::serve(&tokio::runtime::Handle::current(), Arc::clone(&stop), {
        let (stopped, started, release, returned) = (
            Arc::clone(&stop),
            Arc::clone(&started),
            Arc::clone(&release),
            Arc::clone(&returned),
        );
        async move {
            started.notify_one();
            stopped.notified().await; // told to stop …
            release.notified().await; // … but finishing the batch in hand
            returned.fetch_add(1, Ordering::SeqCst);
        }
    });
    started.notified().await;
    let cancel = subscription.cancel();
    tokio::pin!(cancel);
    assert!(
        timeout(SOON, &mut cancel).await.is_err(),
        "the cancel waits for the loop"
    );
    assert_eq!(returned.load(Ordering::SeqCst), 0);
    release.notify_one();
    timeout(SOON, &mut cancel)
        .await
        .expect("the cancel returns once the loop has");
    assert_eq!(returned.load(Ordering::SeqCst), 1);
    timeout(SOON, subscription.cancel())
        .await
        .expect("a second cancel returns at once");
}
