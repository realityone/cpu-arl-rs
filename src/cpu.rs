use std::sync::atomic::{AtomicI8, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time;
use sysinfo;
use thiserror;
use tokio::runtime;

#[derive(Debug)]
pub struct CPUStat {
    pub usage: f64,
}

#[derive(Debug)]
pub struct CPUInfo {
    pub frequency: u64,
    pub quota: f64,
}

pub trait CPUStatProvider {
    fn refresh_cpu_stat(&mut self);
    fn get_cpu_stat(&self) -> CPUStat;
    fn get_cpu_info(&self) -> CPUInfo;
}

pub struct MachineCPUStatProvider {
    sys_cpu: sysinfo::System,

    frequency: u64,
    cpu_cores: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum MachineCPUStatProviderError {
    #[error("Invalid CPU frequency")]
    InvalidCPUFrequencyError,
}

impl MachineCPUStatProvider {
    pub fn new() -> Result<Self, MachineCPUStatProviderError> {
        let mut sys = sysinfo::System::new();
        sys.refresh_all();
        let cpu_cores = sys.cpus().len() as u64;
        let frequency = sys
            .cpus()
            .first()
            .map(|cpu| cpu.frequency())
            .ok_or(MachineCPUStatProviderError::InvalidCPUFrequencyError)?;
        Ok(Self {
            sys_cpu: sys,
            frequency: frequency,
            cpu_cores: cpu_cores,
        })
    }
}

impl CPUStatProvider for MachineCPUStatProvider {
    fn refresh_cpu_stat(&mut self) {
        self.sys_cpu.refresh_cpu_usage();
    }

    fn get_cpu_stat(&self) -> CPUStat {
        CPUStat {
            usage: self.sys_cpu.global_cpu_usage() as f64 * 10.0f64,
        }
    }

    fn get_cpu_info(&self) -> CPUInfo {
        CPUInfo {
            frequency: self.frequency,
            quota: self.cpu_cores as f64,
        }
    }
}

pub struct EMACPUUsageLoader {
    provider: Arc<RwLock<Box<dyn CPUStatProvider + Send + Sync>>>,
    last: Arc<AtomicU64>,
}

impl EMACPUUsageLoader {
    pub fn new(provider: Box<dyn CPUStatProvider + Send + Sync>) -> Self {
        Self {
            provider: Arc::new(RwLock::new(provider)),
            last: Arc::new(AtomicU64::new(0)),
        }
    }

    fn decay() -> f64 {
        0.95
    }

    // calling this function periodically to refresh the CPU usage
    pub fn refresh_cpu_usage(&self) {
        self.provider.write().unwrap().refresh_cpu_stat();
        let current_usage = self.provider.read().unwrap().get_cpu_stat().usage;
        let prev_cpu_usage = self.last.load(Ordering::Relaxed) as f64;

        let current_ema_usage =
            prev_cpu_usage * Self::decay() + current_usage * (1.0 - Self::decay());
        self.last.store(current_ema_usage as u64, Ordering::Relaxed);
    }

    pub fn get_cpu_usage(&self) -> f64 {
        self.last.load(Ordering::Relaxed) as f64
    }
}

mod test {
    use super::*;

    #[test]
    fn machine_cpu_stat_provider() {
        let mut provider = MachineCPUStatProvider::new().unwrap();
        {
            for _ in 0..5 {
                provider.refresh_cpu_stat();
                std::thread::sleep(std::time::Duration::from_millis(500));
                provider.refresh_cpu_stat();

                let stat = provider.get_cpu_stat();
                println!("CPU stat: {:?}", stat);
                assert!(stat.usage > 0.0);
            }
        }

        {
            let info = provider.get_cpu_info();
            println!("CPU info: {:?}", info);
            assert!(info.frequency > 0);
            assert!(info.quota > 0.0);
        }
    }

    #[tokio::test]
    async fn async_load_machine_cpu_stat() {
        let provider = MachineCPUStatProvider::new().unwrap();
        let loader = Arc::new(EMACPUUsageLoader::new(Box::new(provider)));

        {
            let loader = Arc::clone(&loader);
            loader.refresh_cpu_usage();
            tokio::time::sleep(time::Duration::from_millis(500)).await;
            loader.refresh_cpu_usage();
            tokio::time::sleep(time::Duration::from_millis(500)).await;
            tokio::spawn(async move {
                loop {
                    loader.refresh_cpu_usage();
                    tokio::time::sleep(time::Duration::from_millis(500)).await;
                }
            });
        }
        {
            for _ in 0..5 {
                tokio::time::sleep(time::Duration::from_millis(800)).await;
                let usage = loader.get_cpu_usage();
                println!("Async CPU usage: {:?}", usage);
                assert!(usage > 0.0);
            }
        }
    }
}
