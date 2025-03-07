use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time;

use crate::counter;

#[derive(Debug, Clone)]
pub struct StatSnapshot {
    pub cpu: u64,
    pub in_flight: u64,
    pub max_in_flight: u64,
    pub min_rt: u64,
    pub max_pass: u64,
}

#[derive(Debug, Clone)]
struct CounterCache {
    val: u64,
    time: time::Instant,
}

#[derive(Debug, Clone)]
pub enum CPUStatProviderName {
    Machine,
    CGroup,
}

#[derive(Debug, Clone)]
pub struct Options {
    pub window: time::Duration,
    pub bucket: usize,
    pub cpu_threshold: u64,
    pub cpu_quota: f64,
    bucket_duration: time::Duration,
    bucket_per_second: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            window: time::Duration::from_secs(10),
            bucket: 100,
            cpu_threshold: 800,
            cpu_quota: 0.0,
            bucket_duration: time::Duration::from_secs(0),
            bucket_per_second: 0,
        }
    }
}

pub struct ARLLimiter {
    cpu_getter: Box<dyn Fn() -> f64>,
    pass_stat: Box<dyn counter::RollingCounter>,
    rt_stat: Box<dyn counter::RollingCounter>,
    in_flight: AtomicU64,
    prev_drop_time: Mutex<Option<time::Instant>>,
    max_pass_cache: Mutex<Option<CounterCache>>,
    min_rt_cache: Mutex<Option<CounterCache>>,

    opts: Options,
}

impl ARLLimiter {
    pub fn new(cpu_getter: Box<dyn Fn() -> f64>, opts: Options) -> Self {
        let mut opts = opts;
        opts.bucket_duration = opts.window / opts.bucket as u32;
        opts.bucket_per_second =
            (time::Duration::from_secs(1).as_nanos() / opts.bucket_duration.as_nanos()) as u64;
        Self {
            cpu_getter: Box::new(cpu_getter),
            pass_stat: Box::new(counter::RollingCounterStorage::new(
                opts.bucket,
                opts.bucket_duration,
            )),
            rt_stat: Box::new(counter::RollingCounterStorage::new(
                opts.bucket,
                opts.bucket_duration,
            )),
            in_flight: AtomicU64::new(0),
            max_pass_cache: Mutex::new(None),
            min_rt_cache: Mutex::new(None),
            prev_drop_time: Mutex::new(None),
            opts,
        }
    }

    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    fn timespan(&self, last_time: time::Instant) -> usize {
        (last_time.elapsed().as_nanos() / self.opts.bucket_duration.as_nanos()) as usize
    }

    pub fn min_rt(&self) -> u64 {
        if let Some(cache) = self.min_rt_cache.lock().unwrap().as_ref() {
            if self.timespan(cache.time) < 1 {
                return cache.val;
            }
        }

        let raw_min_rt = self
            .rt_stat
            .reduce(
                |iter| {
                    let mut result = std::u16::MAX as f64;
                    for bucket in iter {
                        if bucket.lock().unwrap().points.is_empty() {
                            continue;
                        }
                        let total = bucket.lock().unwrap().points.iter().sum::<f64>();
                        let avg = total / bucket.lock().unwrap().count() as f64;
                        result = result.min(avg);
                        return result;
                    }
                    result
                },
                Some(self.opts.bucket - 1),
            )
            .ceil();

        let raw_min_rt = if raw_min_rt <= 0.0 {
            1
        } else {
            raw_min_rt as u64
        };
        self.min_rt_cache.lock().unwrap().replace(CounterCache {
            val: raw_min_rt,
            time: time::Instant::now(),
        });
        raw_min_rt
    }

    pub fn max_in_flight(&self) -> u64 {
        (((self.max_pass() * self.min_rt() * self.opts.bucket_per_second) as f64) / 1000.0).ceil()
            as u64
    }

    pub fn max_pass(&self) -> u64 {
        if let Some(cache) = self.max_pass_cache.lock().unwrap().as_ref() {
            if self.timespan(cache.time) < 1 {
                return cache.val;
            }
        }

        let raw_max_pass = self.pass_stat.reduce(
            |iter| {
                let mut result = 1.0f64;
                for bucket in iter {
                    let sum = bucket.lock().unwrap().points.iter().sum();
                    result = result.max(sum);
                }
                result
            },
            Some(self.opts.bucket - 1),
        ) as u64;

        self.max_pass_cache.lock().unwrap().replace(CounterCache {
            val: raw_max_pass,
            time: time::Instant::now(),
        });
        raw_max_pass
    }

    pub fn get_cpu_usage(&self) -> u64 {
        (self.cpu_getter)() as u64
    }

    fn should_drop(&self) -> bool {
        let now = time::Instant::now();
        let cpu_usage = self.get_cpu_usage();
        if cpu_usage < self.opts.cpu_threshold {
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
            if prev_drop_time.is_some() {
                return true;
            }
            prev_drop_time.replace(now);
        }
        drop
    }

    pub fn allow(self: &Arc<Self>) -> Result<Box<impl FnMut() + Send + 'static>, ARLLimitError> {
        if self.should_drop() {
            return Err(ARLLimitError::LimitExceededError);
        }
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        let start = time::Instant::now();
        let self_clone = Arc::clone(self);
        Ok(Box::new(move || {
            let rt = start.elapsed().as_millis() as i64;
            if rt > 0 {
                self_clone.rt_stat.add(rt).unwrap_or(());
            }
            self_clone.in_flight.fetch_sub(1, Ordering::Relaxed);
            self_clone.pass_stat.add(1).unwrap_or(());
        }))
    }

    pub fn stat(&self) -> StatSnapshot {
        StatSnapshot {
            cpu: self.get_cpu_usage(),
            min_rt: self.min_rt(),
            max_pass: self.max_pass(),
            max_in_flight: self.max_in_flight(),
            in_flight: self.in_flight.load(Ordering::Relaxed),
        }
    }
}

unsafe impl Sync for ARLLimiter {}
unsafe impl Send for ARLLimiter {}

#[derive(Debug, thiserror::Error)]
pub enum ARLLimitError {
    #[error("CPU ARL Limit Exceeded")]
    LimitExceededError,
}

#[cfg(test)]
mod tests {
    use crate::counter::Metric;

    use super::*;
    use rand::Rng;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_bbr_limiter() {
        let options = Options {
            window: time::Duration::from_secs(5),
            bucket: 50,
            cpu_threshold: 100,
            ..Options::default()
        };
        let test_cpu_getter = Box::new(|| rand::rng().random_range(0.0..30.0) + 80.0);
        let limiter = Arc::new(ARLLimiter::new(test_cpu_getter, options));

        let drop_counter = Arc::new(AtomicU64::new(0));
        let succ_counter = Arc::new(AtomicU64::new(0));
        let mut handles = vec![];

        for _ in 0..100 {
            let limiter = limiter.clone();
            let drop_counter = Arc::clone(&drop_counter);
            let succ_counter = Arc::clone(&succ_counter);
            handles.push(tokio::spawn(async move {
                for _ in 0..300 {
                    let sleep_time = { rand::rng().random_range(1..100) };
                    if let Ok(mut done) = limiter.allow() {
                        succ_counter.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(time::Duration::from_millis(sleep_time)).await;
                        done();
                    } else {
                        drop_counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }
        println!(
            "drop: {}, success: {}",
            drop_counter.load(Ordering::Relaxed),
            succ_counter.load(Ordering::Relaxed)
        );
    }

    #[tokio::test]
    async fn test_bbr_min_rt() {
        let options = Options {
            window: time::Duration::from_secs(1),
            bucket: 10,
            cpu_threshold: 800,
            ..Options::default()
        };
        let test_cpu_getter = Box::new(|| 100.0);
        let bucket_duration = options.window / options.bucket as u32;
        let limiter = ARLLimiter::new(test_cpu_getter, options);

        for i in 0..10 {
            for j in (i * 10 + 1)..=(i * 10 + 10) {
                limiter.rt_stat.add(j).unwrap();
            }
            if i != 9 {
                tokio::time::sleep(bucket_duration).await;
            }
        }
        assert_eq!(limiter.min_rt(), 6);

        let options = Options {
            window: time::Duration::from_secs(1),
            bucket: 10,
            cpu_threshold: 800,
            ..Options::default()
        };
        let bucket_duration = options.window / options.bucket as u32;
        let test_cpu_getter = Box::new(|| 100.0);
        let mut limiter = ARLLimiter::new(test_cpu_getter, options);
        limiter.rt_stat = Box::new(counter::RollingCounterStorage::new(10, bucket_duration));
        assert_eq!(limiter.min_rt(), std::u16::MAX as u64);
    }

    #[tokio::test]
    async fn test_bbr_min_rt_with_cache() {
        let options = Options {
            window: time::Duration::from_secs(1),
            bucket: 10,
            cpu_threshold: 800,
            ..Options::default()
        };
        let test_cpu_getter = Box::new(|| 100.0);
        let bucket_duration = options.window / options.bucket as u32;
        let limiter = ARLLimiter::new(test_cpu_getter, options);

        for i in 0..10 {
            for j in (i * 10 + 1)..=(i * 10 + 5) {
                limiter.rt_stat.add(j).unwrap();
            }
            if i != 9 {
                tokio::time::sleep(bucket_duration / 2).await;
            }
            limiter.min_rt();
            for j in (i * 10 + 6)..=(i * 10 + 10) {
                limiter.rt_stat.add(j).unwrap();
            }
            if i != 9 {
                tokio::time::sleep(bucket_duration / 2).await;
            }
        }
        assert_eq!(limiter.min_rt(), 6);
    }

    #[tokio::test]
    async fn test_bbr_max_qps() {
        let options = Options {
            window: time::Duration::from_secs(1),
            bucket: 10,
            cpu_threshold: 800,
            ..Options::default()
        };
        let bucket_duration = options.window / options.bucket as u32;
        let test_cpu_getter = Box::new(|| 100.0);
        let mut limiter = ARLLimiter::new(test_cpu_getter, options);

        let pass_stat = counter::RollingCounterStorage::new(10, bucket_duration);
        let rt_stat = counter::RollingCounterStorage::new(10, bucket_duration);

        for i in 0..10 {
            pass_stat.add((i + 2) * 100).unwrap();
            for j in (i * 10 + 1)..=(i * 10 + 10) {
                rt_stat.add(j).unwrap();
            }
            if i != 9 {
                tokio::time::sleep(bucket_duration).await;
            }
        }
        limiter.pass_stat = Box::new(pass_stat);
        limiter.rt_stat = Box::new(rt_stat);
        assert_eq!(limiter.max_in_flight(), 60);
    }

    #[tokio::test]
    async fn test_bbr_max_pass() {
        let options = Options {
            window: time::Duration::from_secs(1),
            bucket: 10,
            cpu_threshold: 800,
            ..Options::default()
        };
        let bucket_duration = options.window / options.bucket as u32;
        let test_cpu_getter = Box::new(|| 100.0);
        let limiter = ARLLimiter::new(test_cpu_getter, options);

        for i in 1..=10 {
            limiter.pass_stat.add(i * 100).unwrap();
            tokio::time::sleep(bucket_duration).await;
        }
        assert_eq!(limiter.max_pass(), 1000);

        let options = Options {
            window: time::Duration::from_secs(1),
            bucket: 10,
            cpu_threshold: 800,
            ..Options::default()
        };
        let test_cpu_getter = Box::new(|| 100.0);
        let limiter = ARLLimiter::new(test_cpu_getter, options);
        assert_eq!(limiter.max_pass(), 1);
    }

    #[tokio::test]
    async fn test_bbr_max_pass_with_cache() {
        let options = Options {
            window: time::Duration::from_secs(1),
            bucket: 10,
            cpu_threshold: 800,
            ..Options::default()
        };
        let bucket_duration = options.window / options.bucket as u32;
        let test_cpu_getter = Box::new(|| 100.0);
        let limiter = ARLLimiter::new(test_cpu_getter, options);

        limiter.pass_stat.add(50).unwrap();
        tokio::time::sleep(bucket_duration / 2).await;
        assert_eq!(limiter.max_pass(), 1);
    }

    #[tokio::test]
    async fn test_bbr_should_drop() {
        let options = Options {
            window: time::Duration::from_secs(1),
            bucket: 10,
            cpu_threshold: 800,
            ..Options::default()
        };
        let bucket_duration = options.window / options.bucket as u32;
        let test_cpu_getter = Box::new(|| 100.0);
        let mut limiter = ARLLimiter::new(test_cpu_getter, options);

        let pass_stat = counter::RollingCounterStorage::new(10, bucket_duration);
        let rt_stat = counter::RollingCounterStorage::new(10, bucket_duration);
        for i in 0..10 {
            pass_stat.add((i + 1) * 100).unwrap();
            for j in (i * 10 + 1)..=(i * 10 + 10) {
                rt_stat.add(j).unwrap();
            }
            if i != 9 {
                tokio::time::sleep(bucket_duration).await;
            }
        }
        limiter.pass_stat = Box::new(pass_stat);
        limiter.rt_stat = Box::new(rt_stat);

        limiter.cpu_getter = Box::new(|| 800.0);
        limiter.in_flight.store(50, Ordering::Relaxed);
        assert_eq!(limiter.should_drop(), false);

        limiter.cpu_getter = Box::new(|| 800.0);
        limiter.in_flight.store(80, Ordering::Relaxed);
        assert_eq!(limiter.should_drop(), true);

        limiter.cpu_getter = Box::new(|| 700.0);
        limiter.in_flight.store(80, Ordering::Relaxed);
        assert_eq!(limiter.should_drop(), true);

        tokio::time::sleep(time::Duration::from_secs(2)).await;
        limiter.cpu_getter = Box::new(|| 700.0);
        limiter.in_flight.store(80, Ordering::Relaxed);
        assert_eq!(limiter.should_drop(), false);
    }
}
