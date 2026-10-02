use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
};

pub struct TestAlloc;

static LIVE_BYTES: AtomicI64 = AtomicI64::new(0);
static BASELINE_BYTES: AtomicI64 = AtomicI64::new(0);
static PEAK_BYTES: AtomicU64 = AtomicU64::new(0);
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static DEALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static REALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static MEASURING: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AllocationSnapshot {
    pub live_bytes: i64,
    pub peak_bytes: u64,
    pub allocations: u64,
    pub deallocations: u64,
    pub reallocations: u64,
    pub allocator: &'static str,
}

pub fn reset_measurement() {
    BASELINE_BYTES.store(LIVE_BYTES.load(Ordering::SeqCst), Ordering::SeqCst);
    PEAK_BYTES.store(0, Ordering::SeqCst);
    ALLOCATIONS.store(0, Ordering::SeqCst);
    DEALLOCATIONS.store(0, Ordering::SeqCst);
    REALLOCATIONS.store(0, Ordering::SeqCst);
    MEASURING.store(true, Ordering::SeqCst);
}

pub fn snapshot_measurement() -> AllocationSnapshot {
    AllocationSnapshot {
        live_bytes: LIVE_BYTES
            .load(Ordering::SeqCst)
            .saturating_sub(BASELINE_BYTES.load(Ordering::SeqCst)),
        peak_bytes: PEAK_BYTES.load(Ordering::SeqCst),
        allocations: ALLOCATIONS.load(Ordering::SeqCst),
        deallocations: DEALLOCATIONS.load(Ordering::SeqCst),
        reallocations: REALLOCATIONS.load(Ordering::SeqCst),
        allocator: "runtime-extension-profiles-alloc::TestAlloc",
    }
}

pub fn finish_measurement() -> AllocationSnapshot {
    MEASURING.store(false, Ordering::SeqCst);
    snapshot_measurement()
}

fn observe_live(delta: i64) {
    if !MEASURING.load(Ordering::SeqCst) {
        return;
    }
    let live = LIVE_BYTES
        .fetch_add(delta, Ordering::SeqCst)
        .saturating_add(delta);
    let baseline = BASELINE_BYTES.load(Ordering::SeqCst);
    let measured = live.saturating_sub(baseline);
    if measured > 0 {
        let measured = measured as u64;
        let mut peak = PEAK_BYTES.load(Ordering::SeqCst);
        while measured > peak {
            match PEAK_BYTES.compare_exchange(peak, measured, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => break,
                Err(observed) => peak = observed,
            }
        }
    }
}

unsafe impl GlobalAlloc for TestAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() && MEASURING.load(Ordering::SeqCst) {
            ALLOCATIONS.fetch_add(1, Ordering::SeqCst);
            observe_live(layout.size() as i64);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if !ptr.is_null() && MEASURING.load(Ordering::SeqCst) {
            DEALLOCATIONS.fetch_add(1, Ordering::SeqCst);
            observe_live(-(layout.size() as i64));
        }
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() && MEASURING.load(Ordering::SeqCst) {
            ALLOCATIONS.fetch_add(1, Ordering::SeqCst);
            observe_live(layout.size() as i64);
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let next = unsafe { System.realloc(ptr, layout, new_size) };
        if !next.is_null() && MEASURING.load(Ordering::SeqCst) {
            REALLOCATIONS.fetch_add(1, Ordering::SeqCst);
            observe_live(new_size as i64 - layout.size() as i64);
        }
        next
    }
}
