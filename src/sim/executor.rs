//! A deterministic, single-threaded discrete-event executor built on Rust futures.
//!
//! Every simulated activity (an RPC handler, a queue processor, an SDK poller loop, ...) is an
//! `async` task. Tasks only ever wait on simulated time ([`sleep`]) or on other tasks
//! ([`oneshot`], [`crate::sim::sync`]), so the executor alternates between polling ready tasks
//! and advancing the clock to the next timer. Ties are broken by insertion sequence, which makes a
//! run fully reproducible for a given seed.
//!
//! Time is measured in microseconds of simulated time ([`Time`]).

use std::cell::{Cell, RefCell};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

/// Simulated time in microseconds.
pub type Time = u64;

pub const MICROS_PER_MS: Time = 1_000;
pub const MICROS_PER_SEC: Time = 1_000_000;

type BoxFuture = Pin<Box<dyn Future<Output = ()>>>;

/// Packed task identifier: low 32 bits slot index, high 32 bits generation.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct TaskId(u64);

impl TaskId {
    fn new(slot: u32, generation: u32) -> Self {
        TaskId((u64::from(generation) << 32) | u64::from(slot))
    }
    fn slot(self) -> usize {
        (self.0 & 0xffff_ffff) as usize
    }
    fn generation(self) -> u32 {
        (self.0 >> 32) as u32
    }
}

thread_local! {
    /// Wake-ups are pushed here by wakers and drained by the executor owning this thread.
    static READY: RefCell<VecDeque<TaskId>> = const { RefCell::new(VecDeque::new()) };
    /// Futures spawned while another task is being polled.
    static SPAWNED: RefCell<Vec<BoxFuture>> = const { RefCell::new(Vec::new()) };
    /// Current simulated time, mirrored for cheap access from leaf futures.
    static NOW: Cell<Time> = const { Cell::new(0) };
    /// Timer heap for the executor running on this thread.
    static TIMERS: RefCell<BinaryHeap<TimerEntry>> = const { RefCell::new(BinaryHeap::new()) };
    static TIMER_SEQ: Cell<u64> = const { Cell::new(0) };
}

struct TimerEntry {
    at: Reverse<Time>,
    seq: Reverse<u64>,
    waker: Waker,
}

impl PartialEq for TimerEntry {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at && self.seq == other.seq
    }
}
impl Eq for TimerEntry {}
impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for TimerEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.at, self.seq).cmp(&(other.at, other.seq))
    }
}

// --- waker plumbing -------------------------------------------------------------------------

fn raw_waker(id: TaskId) -> RawWaker {
    RawWaker::new(id.0 as usize as *const (), &VTABLE)
}

unsafe fn clone_waker(data: *const ()) -> RawWaker {
    RawWaker::new(data, &VTABLE)
}
unsafe fn wake(data: *const ()) {
    // SAFETY: `wake_by_ref` only reinterprets `data` as an integer task id.
    unsafe { wake_by_ref(data) }
}
unsafe fn wake_by_ref(data: *const ()) {
    let id = TaskId(data as usize as u64);
    READY.with(|r| r.borrow_mut().push_back(id));
}
unsafe fn drop_waker(_: *const ()) {}

static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_waker, wake, wake_by_ref, drop_waker);

fn waker_for(id: TaskId) -> Waker {
    // SAFETY: the vtable functions never dereference `data`; it is an opaque task id. Wakers
    // are only meaningful on the thread that owns the executor, which is the only place they
    // are ever used (the simulation is single threaded per run).
    unsafe { Waker::from_raw(raw_waker(id)) }
}

// --- public free functions used by model code ----------------------------------------------

/// Current simulated time.
#[inline]
pub fn now() -> Time {
    NOW.with(|n| n.get())
}

/// Spawn a detached task. It is first polled after the current task yields.
pub fn spawn<F: Future<Output = ()> + 'static>(fut: F) {
    SPAWNED.with(|s| s.borrow_mut().push(Box::pin(fut)));
}

fn register_timer(at: Time, waker: Waker) {
    let seq = TIMER_SEQ.with(|s| {
        let v = s.get();
        s.set(v + 1);
        v
    });
    TIMERS.with(|t| {
        t.borrow_mut().push(TimerEntry {
            at: Reverse(at),
            seq: Reverse(seq),
            waker,
        })
    });
}

/// Future that completes once simulated time reaches `deadline`.
pub struct Sleep {
    deadline: Time,
    registered: bool,
}

impl Future for Sleep {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if now() >= self.deadline {
            return Poll::Ready(());
        }
        if !self.registered {
            register_timer(self.deadline, cx.waker().clone());
            self.registered = true;
        }
        Poll::Pending
    }
}

/// Sleep for `dur` microseconds of simulated time. A zero duration still yields once so that
/// other tasks scheduled for the same instant get a chance to run.
pub fn sleep(dur: Time) -> Sleep {
    Sleep {
        deadline: now().saturating_add(dur),
        registered: false,
    }
}

/// Sleep until an absolute simulated time.
pub fn sleep_until(deadline: Time) -> Sleep {
    Sleep {
        deadline,
        registered: false,
    }
}

/// Yield to other runnable tasks without advancing time.
pub fn yield_now() -> YieldNow {
    YieldNow { yielded: false }
}

pub struct YieldNow {
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// Error returned by [`timeout`] when the deadline passes first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Elapsed;

/// Race `fut` against a deadline `dur` from now. The inner future is dropped on timeout, so it
/// must clean up after itself on drop (all primitives in this crate do, lazily).
pub fn timeout<F: Future>(dur: Time, fut: F) -> Timeout<F> {
    Timeout {
        inner: Box::pin(fut),
        sleep: sleep(dur),
    }
}

pub struct Timeout<F: Future> {
    inner: Pin<Box<F>>,
    sleep: Sleep,
}

impl<F: Future> Future for Timeout<F> {
    type Output = Result<F::Output, Elapsed>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Poll::Ready(v) = self.inner.as_mut().poll(cx) {
            return Poll::Ready(Ok(v));
        }
        match Pin::new(&mut self.sleep).poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(Elapsed)),
            Poll::Pending => Poll::Pending,
        }
    }
}

// --- oneshot channel --------------------------------------------------------------------------

struct OneshotInner<T> {
    value: Option<T>,
    waker: Option<Waker>,
    sender_alive: bool,
    receiver_alive: bool,
}

/// Sending half of a [`oneshot`] channel.
pub struct Sender<T> {
    inner: Rc<RefCell<OneshotInner<T>>>,
}

/// Receiving half of a [`oneshot`] channel; resolves to `None` if the sender is dropped.
pub struct Receiver<T> {
    inner: Rc<RefCell<OneshotInner<T>>>,
}

pub fn oneshot<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Rc::new(RefCell::new(OneshotInner {
        value: None,
        waker: None,
        sender_alive: true,
        receiver_alive: true,
    }));
    (
        Sender {
            inner: inner.clone(),
        },
        Receiver { inner },
    )
}

impl<T> Sender<T> {
    /// Deliver a value. Returns it back if the receiver has gone away (e.g. timed out).
    pub fn send(self, value: T) -> Result<(), T> {
        let waker = {
            let mut inner = self.inner.borrow_mut();
            if !inner.receiver_alive {
                return Err(value);
            }
            inner.value = Some(value);
            inner.waker.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
        Ok(())
    }

    /// True if the receiving side has been dropped.
    pub fn is_canceled(&self) -> bool {
        !self.inner.borrow().receiver_alive
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let waker = {
            let mut inner = self.inner.borrow_mut();
            inner.sender_alive = false;
            inner.waker.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.inner.borrow_mut().receiver_alive = false;
    }
}

impl<T> Future for Receiver<T> {
    type Output = Option<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let mut inner = self.inner.borrow_mut();
        if let Some(v) = inner.value.take() {
            return Poll::Ready(Some(v));
        }
        if !inner.sender_alive {
            return Poll::Ready(None);
        }
        inner.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

// --- executor ----------------------------------------------------------------------------------

struct Slot {
    generation: u32,
    fut: Option<BoxFuture>,
    live: bool,
}

/// Owns all tasks of one simulation run. Exactly one executor may run per thread at a time.
pub struct Executor {
    slots: Vec<Slot>,
    free: Vec<u32>,
    live_tasks: usize,
    polls: u64,
}

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

impl Executor {
    /// Create an executor and reset this thread's scheduler state.
    pub fn new() -> Self {
        NOW.with(|n| n.set(0));
        READY.with(|r| r.borrow_mut().clear());
        SPAWNED.with(|s| s.borrow_mut().clear());
        TIMERS.with(|t| t.borrow_mut().clear());
        TIMER_SEQ.with(|s| s.set(0));
        Executor {
            slots: Vec::new(),
            free: Vec::new(),
            live_tasks: 0,
            polls: 0,
        }
    }

    pub fn spawn<F: Future<Output = ()> + 'static>(&mut self, fut: F) {
        self.insert(Box::pin(fut));
    }

    fn insert(&mut self, fut: BoxFuture) {
        let slot = if let Some(slot) = self.free.pop() {
            let s = &mut self.slots[slot as usize];
            s.generation = s.generation.wrapping_add(1);
            s.fut = Some(fut);
            s.live = true;
            slot
        } else {
            self.slots.push(Slot {
                generation: 0,
                fut: Some(fut),
                live: true,
            });
            (self.slots.len() - 1) as u32
        };
        self.live_tasks += 1;
        let id = TaskId::new(slot, self.slots[slot as usize].generation);
        READY.with(|r| r.borrow_mut().push_back(id));
    }

    fn drain_spawned(&mut self) {
        loop {
            let batch = SPAWNED.with(|s| std::mem::take(&mut *s.borrow_mut()));
            if batch.is_empty() {
                break;
            }
            for fut in batch {
                self.insert(fut);
            }
        }
    }

    fn poll_task(&mut self, id: TaskId) {
        let slot_idx = id.slot();
        let Some(slot) = self.slots.get_mut(slot_idx) else {
            return;
        };
        if !slot.live || slot.generation != id.generation() {
            return; // stale wake-up for a finished task
        }
        let Some(mut fut) = slot.fut.take() else {
            return; // already being polled (re-entrant wake), will be re-polled
        };
        let waker = waker_for(id);
        let mut cx = Context::from_waker(&waker);
        self.polls += 1;
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(()) => {
                let slot = &mut self.slots[slot_idx];
                slot.live = false;
                self.free.push(slot_idx as u32);
                self.live_tasks -= 1;
            }
            Poll::Pending => {
                self.slots[slot_idx].fut = Some(fut);
            }
        }
    }

    /// Run until no work remains or simulated time would pass `end`.
    /// Returns the simulated time at which the run stopped.
    pub fn run_until(&mut self, end: Time) -> Time {
        loop {
            self.drain_spawned();
            loop {
                let next = READY.with(|r| r.borrow_mut().pop_front());
                let Some(id) = next else { break };
                self.poll_task(id);
                self.drain_spawned();
            }
            // Advance the clock to the next timer.
            let entry = TIMERS.with(|t| {
                let mut heap = t.borrow_mut();
                match heap.peek() {
                    Some(e) if e.at.0 <= end => heap.pop(),
                    _ => None,
                }
            });
            match entry {
                Some(e) => {
                    let t = e.at.0;
                    if t > now() {
                        NOW.with(|n| n.set(t));
                    }
                    e.waker.wake();
                }
                None => {
                    NOW.with(|n| n.set(end.max(now())));
                    return now();
                }
            }
        }
    }

    pub fn live_tasks(&self) -> usize {
        self.live_tasks
    }

    pub fn polls(&self) -> u64 {
        self.polls
    }

    pub fn pending_timers(&self) -> usize {
        TIMERS.with(|t| t.borrow().len())
    }
}

impl Drop for Executor {
    fn drop(&mut self) {
        // Drop all futures (and the Rc cycles they may hold) before clearing thread state.
        for slot in &mut self.slots {
            slot.fut = None;
        }
        SPAWNED.with(|s| s.borrow_mut().clear());
        TIMERS.with(|t| t.borrow_mut().clear());
        READY.with(|r| r.borrow_mut().clear());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sleeps_advance_time_in_order() {
        let log = Rc::new(RefCell::new(Vec::new()));
        let mut ex = Executor::new();
        for (i, d) in [30u64, 10, 20, 10].into_iter().enumerate() {
            let log = log.clone();
            ex.spawn(async move {
                sleep(d).await;
                log.borrow_mut().push((i, now()));
            });
        }
        ex.run_until(1_000);
        assert_eq!(*log.borrow(), vec![(1, 10), (3, 10), (2, 20), (0, 30)]);
    }

    #[test]
    fn oneshot_and_timeout() {
        let out = Rc::new(RefCell::new(Vec::new()));
        let mut ex = Executor::new();
        let (tx, rx) = oneshot::<u32>();
        let (tx2, rx2) = oneshot::<u32>();
        {
            let out = out.clone();
            ex.spawn(async move {
                let r = timeout(50, rx).await;
                out.borrow_mut().push(format!("a:{:?}@{}", r, now()));
                let r2 = timeout(50, rx2).await;
                out.borrow_mut().push(format!("b:{:?}@{}", r2, now()));
            });
        }
        ex.spawn(async move {
            sleep(20).await;
            let _ = tx.send(7);
            sleep(200).await;
            // receiver already timed out -> value comes back
            assert!(tx2.is_canceled());
            assert_eq!(tx2.send(9), Err(9));
        });
        ex.run_until(10_000);
        assert_eq!(
            *out.borrow(),
            vec![
                "a:Ok(Some(7))@20".to_string(),
                "b:Err(Elapsed)@70".to_string()
            ]
        );
    }

    #[test]
    fn spawn_from_task() {
        let count = Rc::new(Cell::new(0));
        let mut ex = Executor::new();
        let c = count.clone();
        ex.spawn(async move {
            for _ in 0..10 {
                let c = c.clone();
                spawn(async move {
                    sleep(5).await;
                    c.set(c.get() + 1);
                });
            }
        });
        ex.run_until(100);
        assert_eq!(count.get(), 10);
        assert_eq!(ex.live_tasks(), 0);
    }
}
