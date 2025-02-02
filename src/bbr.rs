use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc, Mutex,
};
use std::time;

use crate::counter;

#[derive(Debug, Clone)]
struct StatSnapshot {
    cpu: i64,
    in_flight: i64,
    max_in_flight: i64,
    min_rt: i64,
    max_pass: i64,
}

#[derive(Debug, Clone)]
struct CounterCache {
    val: i64,
    time: time::Instant,
}

#[derive(Debug, Clone)]
struct Options {
    window: time::Duration,
    bucket: usize,
    cpu_threshold: i64,
    cpu_quota: f64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            window: time::Duration::from_secs(10),
            bucket: 100,
            cpu_threshold: 800,
            cpu_quota: 0.0,
        }
    }
}

pub struct BBR {
    cpu: Arc<AtomicI64>,
    pass_stat: Box<dyn counter::RollingCounter>,
    rt_stat: Box<dyn counter::RollingCounter>,
    in_flight: Arc<AtomicI64>,
    bucket_per_second: i64,
    bucket_duration: time::Duration,

    prev_drop_time: Mutex<Option<time::Instant>>,
    max_pass_cache: Mutex<Option<CounterCache>>,
    min_rt_cache: Mutex<Option<CounterCache>>,

    opts: Options,
}

impl BBR {
    fn new(opts: Options) -> Self {
        let bucket_duration = opts.window / opts.bucket as u32;
        Self {
            cpu: Arc::new(AtomicI64::new(0)),
            pass_stat: Box::new(counter::RollingCounterStorage::new(
                opts.bucket,
                bucket_duration,
            )),
            rt_stat: Box::new(counter::RollingCounterStorage::new(
                opts.bucket,
                bucket_duration,
            )),
            in_flight: Arc::new(AtomicI64::new(0)),
            opts,
            max_pass_cache: Mutex::new(None),
            min_rt_cache: Mutex::new(None),
            prev_drop_time: Mutex::new(None),
            bucket_per_second: (time::Duration::from_secs(1).as_nanos()
                / bucket_duration.as_nanos()) as i64,
            bucket_duration,
        }
    }

    fn max_pass(&self) -> i64 {
        let mut cache = self.max_pass_cache.lock().unwrap();
        if let Some(ref cache_val) = *cache {
            if cache_val.time.elapsed() < self.bucket_duration {
                return cache_val.val;
            }
        }
        let raw_max_pass = 1; // Placeholder for actual max pass calculation
        *cache = Some(CounterCache {
            val: raw_max_pass,
            time: time::Instant::now(),
        });
        raw_max_pass
    }

    fn min_rt(&self) -> i64 {
        let mut cache = self.min_rt_cache.lock().unwrap();
        if let Some(ref cache_val) = *cache {
            if cache_val.time.elapsed() < self.bucket_duration {
                return cache_val.val;
            }
        }
        let raw_min_rt = 1; // Placeholder for actual min RT calculation
        *cache = Some(CounterCache {
            val: raw_min_rt,
            time: time::Instant::now(),
        });
        raw_min_rt
    }

    fn max_in_flight(&self) -> i64 {
        (((self.max_pass() * self.min_rt() * self.bucket_per_second) as f64) / 1000.0).ceil() as i64
    }

    fn should_drop(&self) -> bool {
        let now = time::Instant::now();
        let cpu = self.cpu.load(Ordering::Relaxed);
        if cpu < self.opts.cpu_threshold {
            let mut prev_drop_time = self.prev_drop_time.lock().unwrap();
            if prev_drop_time.is_none() {
                return false;
            }
            if now.duration_since(prev_drop_time.unwrap()) <= time::Duration::from_secs(1) {
                let in_flight = self.in_flight.load(Ordering::Relaxed);
                return in_flight > 1 && in_flight > self.max_in_flight();
            }
            *prev_drop_time = None;
            return false;
        }
        let in_flight = self.in_flight.load(Ordering::Relaxed);
        let drop = in_flight > 1 && in_flight > self.max_in_flight();
        if drop {
            let mut prev_drop_time = self.prev_drop_time.lock().unwrap();
            if prev_drop_time.is_none() {
                *prev_drop_time = Some(now);
            }
        }
        drop
    }

    fn allow(&self) -> Result<impl FnOnce() + use<'_>, &'static str> {
        if self.should_drop() {
            return Err("Limit Exceeded");
        }
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        let start = time::Instant::now();
        Ok(move || {
            let rt = start.elapsed().as_millis() as i64;
            if rt > 0 {
                // self.
            }
            self.in_flight.fetch_sub(1, Ordering::Relaxed);
        })
    }

    fn stat(&self) -> StatSnapshot {
        StatSnapshot {
            cpu: self.cpu.load(Ordering::Relaxed),
            min_rt: self.min_rt(),
            max_pass: self.max_pass(),
            max_in_flight: self.max_in_flight(),
            in_flight: self.in_flight.load(Ordering::Relaxed),
        }
    }
}

// fn main() {
//     let limiter = BBR::new(Options::default());

//     match limiter.allow() {
//         Ok(done) => {
//             println!("Request allowed");
//             done();
//         }
//         Err(e) => println!("Request denied: {}", e),
//     }
// }
