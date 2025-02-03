use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub(crate) struct Bucket {
    pub points: Vec<f64>,
    count: usize,
    next: Option<Arc<Mutex<Bucket>>>,
}

impl Bucket {
    fn new() -> Self {
        Self {
            points: Vec::new(),
            count: 0,
            next: None,
        }
    }

    pub fn count(&self) -> usize {
        self.count
    }

    fn append(&mut self, val: f64) {
        self.points.push(val);
        self.count += 1;
    }

    fn add(&mut self, offset: usize, val: f64) {
        if offset < self.points.len() {
            self.points[offset] += val;
            self.count += 1;
        }
    }

    fn reset(&mut self) {
        self.points.clear();
        self.count = 0;
    }

    fn next(&self) -> Option<Arc<Mutex<Bucket>>> {
        self.next.clone()
    }
}

#[derive(Debug)]
pub(crate) struct Window {
    pub buckets: Vec<Arc<Mutex<Bucket>>>,
    size: usize,
}

impl Window {
    pub fn new(size: usize) -> Self {
        let buckets: Vec<Arc<Mutex<Bucket>>> = (0..size)
            .map(|_| Arc::new(Mutex::new(Bucket::new())))
            .collect();

        for i in 0..size {
            let next_index = (i + 1) % size;
            let next_bucket = buckets[next_index].clone();
            buckets[i].lock().unwrap().next = Some(next_bucket.clone());
        }

        Self {
            buckets,
            size: size,
        }
    }

    pub fn reset_window(&mut self) {
        for bucket in &self.buckets {
            bucket.lock().unwrap().reset();
        }
    }

    pub fn reset_bucket(&mut self, offset: usize) {
        self.buckets[offset % self.size].lock().unwrap().reset();
    }

    pub fn reset_buckets(&mut self, offset: usize, count: usize) {
        for i in 0..count {
            self.reset_bucket(offset + i);
        }
    }

    pub fn append(&mut self, offset: usize, val: f64) {
        self.buckets[offset % self.size].lock().unwrap().append(val);
    }

    pub fn add(&mut self, offset: usize, val: f64) {
        let idx = offset % self.size;
        let mut bucket = self.buckets[idx].lock().unwrap();
        if bucket.count == 0 {
            bucket.append(val);
        } else {
            bucket.add(0, val);
        }
    }

    pub fn bucket(&self, offset: usize) -> Arc<Mutex<Bucket>> {
        self.buckets[offset % self.size].clone()
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn iterator(&self, offset: usize, count: usize) -> BucketIterator {
        BucketIterator::from_window(self, offset, count)
    }
}

pub(crate) struct BucketIterator {
    count: usize,
    iterated_count: usize,
    cur: Option<Arc<Mutex<Bucket>>>,
}

impl BucketIterator {
    fn from_window(window: &Window, offset: usize, count: usize) -> Self {
        Self {
            count,
            iterated_count: 0,
            cur: Some(window.buckets[offset % window.size].clone()),
        }
    }
}

impl Iterator for BucketIterator {
    type Item = Arc<Mutex<Bucket>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.iterated_count >= self.count {
            return None;
        }
        if let Some(current) = self.cur.take() {
            let next_bucket = current.lock().unwrap().next.clone();
            self.cur = next_bucket;
            self.iterated_count += 1;
            return Some(current);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_window_reset_window() {
        let size = 3;
        let mut window = Window::new(size);
        for i in 0..size {
            window.append(i, 1.0);
        }
        window.reset_window();
        for i in 0..size {
            assert_eq!(window.bucket(i).lock().unwrap().points.len(), 0);
        }
    }

    #[test]
    fn test_window_reset_bucket() {
        let size = 3;
        let mut window = Window::new(size);
        for i in 0..size {
            window.append(i, 1.0);
        }
        window.reset_bucket(1);
        assert_eq!(window.bucket(1).lock().unwrap().points.len(), 0);
        assert_eq!(window.bucket(0).lock().unwrap().points[0], 1.0);
        assert_eq!(window.bucket(2).lock().unwrap().points[0], 1.0);
    }

    #[test]
    fn test_window_reset_buckets() {
        let size = 3;
        let mut window = Window::new(size);
        for i in 0..size {
            window.append(i, 1.0);
        }
        window.reset_buckets(0, 3);
        for i in 0..size {
            assert_eq!(window.bucket(i).lock().unwrap().points.len(), 0);
        }
    }

    #[test]
    fn test_window_append() {
        let size = 3;
        let mut window = Window::new(size);
        for i in 0..size {
            window.append(i, 1.0);
        }
        for i in 0..size {
            assert_eq!(window.bucket(i).lock().unwrap().points[0], 1.0);
        }
    }

    #[test]
    fn test_window_add() {
        let size = 3;
        let mut window = Window::new(size);
        window.append(0, 1.0);
        window.add(0, 1.0);
        assert_eq!(window.bucket(0).lock().unwrap().points[0], 2.0);
    }

    #[test]
    fn test_window_size() {
        let size = 3;
        let window = Window::new(size);
        assert_eq!(window.size(), 3);
    }
}
