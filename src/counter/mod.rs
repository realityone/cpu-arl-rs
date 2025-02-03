use std::sync::{Arc, Mutex};
use std::time;

pub(crate) mod policy;
pub(crate) mod window;

pub trait Metric {
    fn add(&self, value: i64) -> Result<(), String>;
    fn value(&self) -> i64;
}

pub trait Aggregation {
    fn min(&self) -> f64;
    fn max(&self) -> f64;
    fn avg(&self) -> f64;
    fn sum(&self) -> f64;
}

pub trait RollingCounter: Metric + Aggregation {
    fn timespan(&self) -> usize;
    fn reduce(&self, f: fn(&mut window::BucketIterator) -> f64) -> f64;
}

pub struct RollingCounterStorage {
    policy: Arc<Mutex<policy::RollingPolicy>>,
}

impl RollingCounterStorage {
    pub fn new(size: usize, bucket_duration: time::Duration) -> Self {
        let window = window::Window::new(size);
        let policy = policy::RollingPolicy::new(window, bucket_duration);
        Self {
            policy: Arc::new(Mutex::new(policy)),
        }
    }
}

fn min(iter: &mut window::BucketIterator) -> f64 {
    let mut result = 0.0;
    let mut started = false;
    for bucket in iter {
        for &p in bucket.lock().unwrap().points.iter() {
            if !started {
                result = p;
                started = true;
                continue;
            }
            if p < result {
                result = p;
            }
        }
    }
    return result;
}

fn max(iter: &mut window::BucketIterator) -> f64 {
    let mut result = 0.0;
    let mut started = false;
    for bucket in iter {
        for &p in bucket.lock().unwrap().points.iter() {
            if !started {
                result = p;
                started = true;
                continue;
            }
            if p > result {
                result = p;
            }
        }
    }
    return result;
}

fn avg(iter: &mut window::BucketIterator) -> f64 {
    let mut sum = 0.0;
    let mut count = 0usize;
    for bucket in iter {
        count += bucket.lock().unwrap().points.iter().count();
        for &p in bucket.lock().unwrap().points.iter() {
            sum += p;
        }
    }
    if count == 0 {
        return 0.0;
    }
    return sum / count as f64;
}

fn sum(iter: &mut window::BucketIterator) -> f64 {
    let mut result = 0.0;
    for bucket in iter {
        for &p in bucket.lock().unwrap().points.iter() {
            result += p;
        }
    }
    return result;
}

impl Aggregation for RollingCounterStorage {
    fn min(&self) -> f64 {
        let policy = self.policy.lock().unwrap();
        policy.reduce(min)
    }
    fn max(&self) -> f64 {
        let policy = self.policy.lock().unwrap();
        policy.reduce(max)
    }
    fn avg(&self) -> f64 {
        let policy = self.policy.lock().unwrap();
        policy.reduce(avg)
    }
    fn sum(&self) -> f64 {
        let policy = self.policy.lock().unwrap();
        policy.reduce(sum)
    }
}

impl Metric for RollingCounterStorage {
    fn add(&self, value: i64) -> Result<(), String> {
        if value < 0 {
            return Err(format!(
                "stat/metric: cannot decrease in value. val: {}",
                value
            ));
        }
        let mut policy = self.policy.lock().unwrap();
        policy.add(value as f64);
        Ok(())
    }

    fn value(&self) -> i64 {
        self.sum() as i64
    }
}

impl RollingCounter for RollingCounterStorage {
    fn timespan(&self) -> usize {
        let policy = self.policy.lock().unwrap();
        policy.timespan()
    }

    fn reduce(&self, f: fn(&mut window::BucketIterator) -> f64) -> f64 {
        let policy = self.policy.lock().unwrap();
        policy.reduce(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::RwLock;
    use std::time::Duration;

    #[test]
    fn test_rolling_counter_add() {
        let r = RollingCounterStorage::new(3, Duration::from_secs(1));
        r.add(1).unwrap();
        assert_eq!(r.value(), 1);
    }

    #[test]
    fn test_rolling_counter_reduce() {
        let r = RollingCounterStorage::new(3, Duration::from_secs(1));
        r.add(1).unwrap();
        r.add(2).unwrap();
        r.add(3).unwrap();
        assert_eq!(r.sum(), 6.0);
    }

    #[tokio::test]
    async fn test_rolling_counter_data_race() {
        use tokio;
        use tokio::sync::broadcast;

        let (tx, _) = broadcast::channel::<()>(1);

        let r = Arc::new(RwLock::new(RollingCounterStorage::new(
            3,
            Duration::from_secs(1),
        )));
        let r1 = Arc::clone(&r);
        let r2 = Arc::clone(&r);

        let mut rx1 = tx.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = rx1.recv() => {
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(5)) => {
                        r1.clone().write().unwrap().add(1).unwrap();
                    }
                }
            }
        });

        let mut rx2 = tx.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = rx2.recv() => {
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(200)) => {
                        let sum = r2.clone().write().unwrap().reduce(sum);
                        assert!(sum > 0.0);
                    }
                }
            }
        });

        tokio::time::sleep(Duration::from_secs(3)).await;
        tx.send(()).unwrap();
    }
}
