use std::time;
use sysinfo;
use thiserror;

pub(crate) const DEFAULT_MINIMUM_CPU_UPDATE_INTERVAL: time::Duration =
    time::Duration::from_millis(500);

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
    fn get_cpu_stat(&mut self) -> CPUStat;
    // async fn async_get_cpu_stat(&mut self) -> CPUStat;
    fn async_get_cpu_stat(&mut self) -> impl std::future::Future<Output = CPUStat> + Send;
    fn get_cpu_info(&self) -> CPUInfo;
}

pub struct MachineCPUStatProvider {
    cpu_update_interval: time::Duration,
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
            cpu_update_interval: DEFAULT_MINIMUM_CPU_UPDATE_INTERVAL,
            sys_cpu: sys,
            frequency: frequency,
            cpu_cores: cpu_cores,
        })
    }

    pub fn set_cpu_update_interval(&mut self, interval: time::Duration) {
        self.cpu_update_interval = interval;
    }
}

impl CPUStatProvider for MachineCPUStatProvider {
    fn get_cpu_stat(&mut self) -> CPUStat {
        self.sys_cpu.refresh_cpu_usage();
        std::thread::sleep(self.cpu_update_interval);
        self.sys_cpu.refresh_cpu_usage();
        CPUStat {
            usage: self.sys_cpu.global_cpu_usage() as f64,
        }
    }

    async fn async_get_cpu_stat(&mut self) -> CPUStat {
        self.sys_cpu.refresh_cpu_usage();
        tokio::time::sleep(self.cpu_update_interval).await;
        self.sys_cpu.refresh_cpu_usage();
        CPUStat {
            usage: self.sys_cpu.global_cpu_usage() as f64,
        }
    }

    fn get_cpu_info(&self) -> CPUInfo {
        CPUInfo {
            frequency: self.frequency,
            quota: self.cpu_cores as f64,
        }
    }
}

mod test {

    #[test]
    fn machine_cpu_stat_provider() {
        use crate::cpu::CPUStatProvider;

        let mut provider = super::MachineCPUStatProvider::new().unwrap();
        {
            for _ in 0..5 {
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
    async fn async_machine_cpu_stat_provider() {
        use crate::cpu::CPUStatProvider;
        let mut provider = super::MachineCPUStatProvider::new().unwrap();
        {
            for _ in 0..5 {
                let stat = provider.async_get_cpu_stat().await;
                println!("Async CPU stat: {:?}", stat);
                assert!(stat.usage > 0.0);
            }
        }
    }
}
