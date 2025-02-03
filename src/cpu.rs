use std::sync::{Arc, RwLock};
use std::time;
use sysinfo;
use thiserror;

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

pub struct AsyncEMACPUUsageLoader {
    handle: tokio::runtime::Handle,
    provider: Arc<RwLock<Box<dyn CPUStatProvider + Send + Sync>>>,
    last: Arc<RwLock<f64>>,
    ticker_interval: time::Duration,
    stop_tx: Option<tokio::sync::mpsc::Sender<()>>,
}

impl AsyncEMACPUUsageLoader {
    pub fn new(
        handle: tokio::runtime::Handle,
        provider: Box<dyn CPUStatProvider + Send + Sync>,
        ticker_interval: time::Duration,
    ) -> Self {
        Self {
            handle,
            provider: Arc::new(RwLock::new(provider)),
            last: Arc::new(RwLock::new(0.0)),
            ticker_interval,
            stop_tx: None,
        }
    }

    fn decay() -> f64 {
        0.95
    }

    pub fn start(&mut self) {
        let last_ptr = self.last.clone();
        let provider = self.provider.clone();
        let mut ticker = tokio::time::interval(self.ticker_interval);
        let (stop_tx, mut stop_rx) = tokio::sync::mpsc::channel::<()>(1);
        self.stop_tx = Some(stop_tx);
        self.handle.spawn(async move {
            provider.write().unwrap().refresh_cpu_stat();
            ticker.tick().await;
            provider.write().unwrap().refresh_cpu_stat();

            {
                let current_usage = provider.read().unwrap().get_cpu_stat().usage;
                let prev_cpu_usage = 0.0;
                *last_ptr.write().unwrap() = prev_cpu_usage*Self::decay() + current_usage*(1.0-Self::decay());
            }  
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        provider.write().unwrap().refresh_cpu_stat();
                        let current_usage = provider.read().unwrap().get_cpu_stat().usage;
                        let prev_cpu_usage = *last_ptr.read().unwrap() as f64;
                        *last_ptr.write().unwrap() = prev_cpu_usage*Self::decay() + current_usage*(1.0-Self::decay());
                    }
                    _ = stop_rx.recv() => {
                        return;
                    }
                }
            }
        });
    }

    pub fn stop(&mut self) {
        if let Some(tx) = &self.stop_tx {
            let _ = tx.send(());
        }
    }

    pub fn get_cpu_usage(&self) -> f64 {    
        self.last.read().unwrap().clone()
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
        let mut loader = AsyncEMACPUUsageLoader::new(
            tokio::runtime::Handle::current(),
            Box::new(provider),
            time::Duration::from_millis(500),
        );
        loader.start();
        {
            for _ in 0..5 {
                tokio::time::sleep(time::Duration::from_millis(100)).await;
                let usage = loader.get_cpu_usage();
                println!("Async CPU usage: {:?}", loader.get_cpu_usage());
                assert!(usage > 0.0);
            }
        }
    }
}
