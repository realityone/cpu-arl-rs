use std::path;

pub struct CGroupCPUStatProvider {
    root_path: path::PathBuf,
    v2: bool,
    cpu_update_interval: time::Duration,
    sys_cpu: sysinfo::System,
}

impl CGroupCPUStatProvider {
    pub fn new(root_path: path::PathBuf, v2: bool) -> Self {
        let mut sys = sysinfo::System::new();
        sys.refresh_cpu_frequency();
        Self {
            root_path,
            v2,
            cpu_update_interval: DEFAULT_MINIMUM_CPU_UPDATE_INTERVAL,
            sys_cpu: sys,
        }
    }

    pub fn set_cpu_update_interval(&mut self, interval: time::Duration) {
        self.cpu_update_interval = interval;
    }
}

impl CPUStatProvider for CGroupCPUStatProvider {
    fn get_cpu_stat(&mut self) -> CPUStat {
        CPUStat { usage: 0 }
    }

    async fn async_get_cpu_stat(&mut self) -> CPUStat {
        CPUStat { usage: 0 }
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
