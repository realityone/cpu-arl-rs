use once_cell::sync::Lazy;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex, RwLock,
};
use std::time;

use crate::counter;
use crate::cpu;

#[derive(Debug, Clone)]
struct StatSnapshot {
    cpu: u64,
    in_flight: u64,
    max_in_flight: u64,
    min_rt: u64,
    max_pass: u64,
}

#[derive(Debug, Clone)]
struct CounterCache {
    val: u64,
    time: time::Instant,
}

#[derive(Debug, Clone)]
enum CPUStatProviderName {
    Machine,
    CGroup,
}

#[derive(Debug, Clone)]
struct Options {
    provider: CPUStatProviderName,
    window: time::Duration,
    bucket: usize,
    cpu_threshold: u64,
    cpu_quota: f64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            provider: CPUStatProviderName::Machine,
            window: time::Duration::from_secs(10),
            bucket: 100,
            cpu_threshold: 800,
            cpu_quota: 0.0,
        }
    }
}

pub struct ARLLimiter {
    cpu_getter: Box<dyn Fn() -> f64 + Send + Sync>,
    pass_stat: Box<dyn counter::RollingCounter + Send + Sync>,
    rt_stat: Box<dyn counter::RollingCounter + Send + Sync>,
    in_flight: AtomicU64,
    bucket_per_second: u64,
    bucket_duration: time::Duration,

    prev_drop_time: Mutex<Option<time::Instant>>,
    max_pass_cache: Mutex<Option<CounterCache>>,
    min_rt_cache: Mutex<Option<CounterCache>>,

    opts: Options,
}

static GLOBAL_CPU_LOADER: Lazy<RwLock<Option<cpu::AsyncEMACPUUsageLoader>>> =
    Lazy::new(|| RwLock::new(None));

impl ARLLimiter {
    fn new(opts: Options) -> Self {
        match opts.provider {
            CPUStatProviderName::Machine => {
                let mut loader = cpu::AsyncEMACPUUsageLoader::new(
                    tokio::runtime::Handle::current(),
                    Box::new(cpu::MachineCPUStatProvider::new().unwrap()),
                    time::Duration::from_millis(500),
                );
                loader.start();
                GLOBAL_CPU_LOADER.write().unwrap().replace(loader);
            }
            #[cfg(all(target_os = "linux", feature = "cgroup"))]
            CPUStatProviderName::CGroup => {
                todo!();
            }
            _ => {
                unimplemented!("Unsupported CPU stat provider: {:?}", opts.provider);
            }
        }

        let cpu_getter = || {
            GLOBAL_CPU_LOADER
                .read()
                .unwrap()
                .as_ref()
                .unwrap()
                .get_cpu_usage()
        };
        let bucket_duration = opts.window / opts.bucket as u32;
        Self {
            cpu_getter: Box::new(cpu_getter),
            pass_stat: Box::new(counter::RollingCounterStorage::new(
                opts.bucket,
                bucket_duration,
            )),
            rt_stat: Box::new(counter::RollingCounterStorage::new(
                opts.bucket,
                bucket_duration,
            )),
            in_flight: AtomicU64::new(0),
            opts,
            max_pass_cache: Mutex::new(None),
            min_rt_cache: Mutex::new(None),
            prev_drop_time: Mutex::new(None),
            bucket_per_second: (time::Duration::from_secs(1).as_nanos()
                / bucket_duration.as_nanos()) as u64,
            bucket_duration,
        }
    }

    fn timespan(&self, last_time: time::Instant) -> usize {
        (last_time.elapsed().as_nanos() / self.bucket_duration.as_nanos()) as usize
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
                    let mut result = std::f64::MAX;
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

    fn max_in_flight(&self) -> u64 {
        (((self.max_pass() * self.min_rt() * self.bucket_per_second) as f64) / 1000.0).ceil() as u64
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

    fn get_cpu_usage(&self) -> u64 {
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

    fn allow(&self) -> Result<impl FnOnce() + use<'_>, ARLLimitError> {
        if self.should_drop() {
            return Err(ARLLimitError::LimitExceededError);
        }
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        let start = time::Instant::now();
        Ok(move || {
            let rt = start.elapsed().as_millis() as i64;
            if rt > 0 {
                self.rt_stat.add(rt).unwrap_or(());
            }
            self.in_flight.fetch_sub(1, Ordering::Relaxed);
            self.pass_stat.add(1).unwrap_or(());
        })
    }

    fn stat(&self) -> StatSnapshot {
        StatSnapshot {
            cpu: self.get_cpu_usage(),
            min_rt: self.min_rt(),
            max_pass: self.max_pass(),
            max_in_flight: self.max_in_flight(),
            in_flight: self.in_flight.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum ARLLimitError {
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
    async fn test_bbr() {
        let options = Options {
            window: time::Duration::from_secs(5),
            bucket: 50,
            cpu_threshold: 100,
            ..Options::default()
        };
        let limiter = Arc::new(ARLLimiter::new(options));

        let drop_counter = Arc::new(AtomicU64::new(0));
        let mut handles = vec![];

        for _ in 0..100 {
            let limiter = limiter.clone();
            let drop_counter = Arc::clone(&drop_counter);
            handles.push(tokio::spawn(async move {
                for _ in 0..300 {
                    let sleep_time = { rand::rng().random_range(1..100) };
                    if let Ok(done) = limiter.allow() {
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
        println!("drop: {}", drop_counter.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_bbr_min_rt() {
        let options = Options {
            window: time::Duration::from_secs(1),
            bucket: 10,
            cpu_threshold: 800,
            ..Options::default()
        };
        let bucket_duration = options.window / options.bucket as u32;
        let limiter = ARLLimiter::new(options);

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
        let mut limiter = ARLLimiter::new(options);
        limiter.rt_stat = Box::new(counter::RollingCounterStorage::new(10, bucket_duration));
        assert_eq!(limiter.min_rt(), std::f64::MAX as u64);
    }

    #[tokio::test]
    async fn test_bbr_min_rt_with_cache() {
        let options = Options {
            window: time::Duration::from_secs(1),
            bucket: 10,
            cpu_threshold: 800,
            ..Options::default()
        };
        let bucket_duration = options.window / options.bucket as u32;
        let limiter = ARLLimiter::new(options);

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
        let mut limiter = ARLLimiter::new(options);

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
        let limiter = ARLLimiter::new(options);

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
        let limiter = ARLLimiter::new(options);
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
        let limiter = ARLLimiter::new(options);

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
        let mut limiter = ARLLimiter::new(options);

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
