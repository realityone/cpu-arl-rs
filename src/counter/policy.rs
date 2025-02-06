use super::window::{BucketIterator, Window};
use std::time;

#[derive(Debug)]
pub(crate) struct RollingPolicy {
    window: Window,
    offset: usize,
    last_append_time: time::Instant,
    opts: RollingPolicyOpts,
}

#[derive(Debug)]
struct RollingPolicyOpts {
    bucket_duration: time::Duration,
}

impl RollingPolicy {
    pub fn new(window: Window, bucket_duration: time::Duration) -> Self {
        Self {
            window: window,
            offset: 0,
            last_append_time: time::Instant::now(),
            opts: RollingPolicyOpts {
                bucket_duration: bucket_duration,
            },
        }
    }

    fn size(&self) -> usize {
        self.window.size()
    }

    pub fn timespan(&self) -> usize {
        let elapsed = self.last_append_time.elapsed().as_nanos();
        let span = elapsed / self.opts.bucket_duration.as_nanos();
        span as usize
    }

    fn apply<F>(&mut self, f: F, val: f64)
    where
        F: Fn(&mut Window, usize, f64),
    {
        let timespan = self.timespan();
        let ori_timespan = timespan;

        if timespan > 0 {
            let start = (self.offset + 1) % self.size();
            let end = (self.offset + timespan) % self.size();
            let timespan = timespan.min(self.size());

            self.window.reset_buckets(start, timespan);
            self.offset = end;
            self.last_append_time += self.opts.bucket_duration * ori_timespan as u32;
        }
        f(&mut self.window, self.offset, val);
    }

    fn append(&mut self, val: f64) {
        self.apply(Window::append, val);
    }

    pub fn add(&mut self, val: f64) {
        self.apply(Window::add, val);
    }

    pub fn reduce(&self, f: fn(&mut BucketIterator) -> f64, iter_max: Option<usize>) -> f64 {
        let timespan = self.timespan();
        let count = self.size() as i64 - timespan as i64;
        if count > 0 {
            let mut offset = self.offset + timespan + 1;
            if offset >= self.size() {
                offset -= self.size();
            }
            let iter_count = iter_max.map_or(count as usize, |max| max.min(count as usize));
            let mut iter = self.window.iterator(offset, iter_count);
            return f(&mut iter);
        }
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::RollingPolicy;
    use crate::counter::window::Window;
    use std::thread::sleep;
    use std::time::Duration;

    fn get_rolling_policy() -> RollingPolicy {
        let window = Window::new(3); // Assuming Window::new(size)
        RollingPolicy::new(window, Duration::from_millis(100))
    }

    #[test]
    fn test_rolling_policy_add() {
        let tests = vec![
            (vec![150, 51], vec![1, 2], vec![1, 1]),
            (vec![94, 250], vec![0, 0], vec![1, 1]),
            (vec![150, 300, 600], vec![1, 1, 1], vec![1, 1, 1]),
        ];

        for (time_sleep, offset, points) in tests {
            let mut policy = get_rolling_policy();
            let mut total_ts = 0;
            let mut last_offset = 0;

            for (i, &n) in time_sleep.iter().enumerate() {
                total_ts += n;
                sleep(Duration::from_millis(n as u64));
                policy.add(points[i] as f64);

                let expected_offset = offset[i];
                let expected_points = points[i];
                let bucket_points = policy.window.buckets[expected_offset]
                    .lock()
                    .unwrap()
                    .points[0] as usize;

                assert_eq!(
                    expected_points, bucket_points,
                    "Error, time since last append: {}ms, last offset: {}",
                    total_ts, last_offset
                );
                last_offset = expected_offset;
            }
        }
    }

    #[test]
    fn test_rolling_policy_add_with_timespan() {
        // Case: timespan < bucket number
        {
            let mut policy = get_rolling_policy();
            policy.add(0.0);
            sleep(Duration::from_millis(101));
            policy.add(1.0);
            sleep(Duration::from_millis(101));
            policy.add(2.0);
            sleep(Duration::from_millis(201));
            policy.add(4.0);

            assert_eq!(policy.window.buckets[0].lock().unwrap().points.len(), 0);
            assert_eq!(
                policy.window.buckets[1].lock().unwrap().points[0] as usize,
                4
            );
            assert_eq!(
                policy.window.buckets[2].lock().unwrap().points[0] as usize,
                2
            );
        }

        // Case: timespan > bucket number
        {
            let mut policy = get_rolling_policy();
            policy.add(0.0);
            sleep(Duration::from_millis(101));
            policy.add(1.0);
            sleep(Duration::from_millis(101));
            policy.add(2.0);
            sleep(Duration::from_millis(501));
            policy.add(4.0);

            assert_eq!(policy.window.buckets[0].lock().unwrap().points.len(), 0);
            assert_eq!(
                policy.window.buckets[1].lock().unwrap().points[0] as usize,
                4
            );
            assert_eq!(policy.window.buckets[2].lock().unwrap().points.len(), 0);
        }
    }
}
