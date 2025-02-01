use std::time;
use sysinfo;

const DEFAULT_MINIMUM_CPU_UPDATE_INTERVAL: time::Duration = time::Duration::from_millis(500);

#[derive(Debug)]
pub struct CPUStat {
    pub usage: u64,
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
}

impl MachineCPUStatProvider {
    pub fn new() -> Self {
        let mut sys = sysinfo::System::new();
        sys.refresh_cpu_frequency();
        Self {
            cpu_update_interval: DEFAULT_MINIMUM_CPU_UPDATE_INTERVAL,
            sys_cpu: sys,
        }
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
            usage: self.sys_cpu.global_cpu_usage() as u64,
        }
    }

    async fn async_get_cpu_stat(&mut self) -> CPUStat {
        self.sys_cpu.refresh_cpu_usage();
        tokio::time::sleep(self.cpu_update_interval).await;
        self.sys_cpu.refresh_cpu_usage();
        CPUStat {
            usage: self.sys_cpu.global_cpu_usage() as u64,
        }
    }

    fn get_cpu_info(&self) -> CPUInfo {
        CPUInfo {
            frequency: self
                .sys_cpu
                .cpus()
                .first()
                .map(|cpu| cpu.frequency())
                .unwrap_or(0),
            quota: self.sys_cpu.cpus().len() as f64,
        }
    }
}

mod test {
    #[test]
    fn machine_cpu_stat_provider() {
        use crate::cpu::CPUStatProvider;

        let mut provider = super::MachineCPUStatProvider::new();
        {
            let stat = provider.get_cpu_stat();
            assert!(stat.usage > 0);
            println!("CPU stat: {:?}", stat);
        }

        {
            let info = provider.get_cpu_info();
            assert!(info.frequency > 0);
            assert!(info.quota > 0.0);
            println!("CPU info: {:?}", info);
        }
    }

    #[tokio::test]
    async fn async_machine_cpu_stat_provider() {
        use crate::cpu::CPUStatProvider;
        let mut provider = super::MachineCPUStatProvider::new();
        let stat = provider.get_cpu_stat();
        assert!(stat.usage > 0);
        println!("Async CPU stat: {:?}", stat);
    }
}
