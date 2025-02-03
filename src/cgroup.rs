use crate::cpu;
use libc;
use std::{
    fs,
    io::{self, BufRead},
    num, path, result,
};
use sysinfo;
use thiserror;

const NANO_SECONDS_PER_SECOND: u64 = 1e9 as u64;

#[derive(Debug, Clone)]
struct CPUUsageItem {
    acct_usage: u64,
    system_usage: u64,
}

#[derive(Debug)]
pub struct CGroupCPUStatProvider {
    root_path: path::PathBuf,
    v2: bool,
    sys_cpu: sysinfo::System,
    meta: CGroupMetadata,
    metrics: [CPUUsageItem; 2],
    refresh_index: usize,
}

#[derive(Debug)]
struct CGroupMetadata {
    quota: f64,
    cores: u64,
    frequency: u64,
    clock_cycle: u64,
}

impl CGroupCPUStatProvider {
    pub fn new(root_path: path::PathBuf, v2: bool) -> Result<Self, StatReadError> {
        let mut sys = sysinfo::System::new();
        sys.refresh_all();
        let frequency = sys
            .cpus()
            .first()
            .map(|cpu| cpu.frequency())
            .ok_or(StatReadError::InvalidCPUFrequencyError)?;
        let cores = { Self::get_cpu_cores(&sys, root_path.clone(), v2)? };
        let quota = {
            let by_cpuset =
                Self::get_cpuset_cpus(root_path.clone(), v2).map(|cpus| cpus.len() as f64)?;
            let by_cfs = Self::get_cfs_cpu_quota_unit_cores(root_path.clone(), v2)?;
            by_cpuset.min(by_cfs)
        };
        let provider = Self {
            root_path,
            v2,
            sys_cpu: sys,
            meta: CGroupMetadata {
                quota: quota,
                cores: cores,
                frequency: frequency,
                clock_cycle: Self::get_clock_cycle(),
            },
            metrics: [
                CPUUsageItem {
                    acct_usage: 0,
                    system_usage: 0,
                },
                CPUUsageItem {
                    acct_usage: 0,
                    system_usage: 0,
                },
            ],
            refresh_index: 0,
        };
        return Ok(provider);
    }

    fn get_cpu_cores(
        sys: &sysinfo::System,
        root_path: path::PathBuf,
        v2: bool,
    ) -> Result<u64, StatReadError> {
        let machine_cpus = sys.cpus().len() as u64;
        if !v2 {
            let usage_percpu_path = root_path.join("cpu/cpuacct.usage_percpu");
            match LinuxStatReader::read_as_u64_array(usage_percpu_path) {
                Ok(usage) => {
                    let cores = usage.iter().filter(|&&v| v > 0).count() as u64;
                    return Ok(cores);
                }
                Err(_) => return Ok(machine_cpus),
            }
        }
        Err(StatReadError::UnimplementedCGroupV2Error)
    }

    fn get_cpuset_cpus(root_path: path::PathBuf, v2: bool) -> Result<Vec<u64>, StatReadError> {
        if !v2 {
            let cpuset_path = root_path.join("cpuset/cpuset.cpus");
            return Ok(LinuxStatReader::read_as_cpuset_cpus_array(cpuset_path)?);
        }
        Err(StatReadError::UnimplementedCGroupV2Error)
    }

    fn get_cfs_cpu_quota_unit_cores(
        root_path: path::PathBuf,
        v2: bool,
    ) -> Result<f64, StatReadError> {
        if !v2 {
            let quota_path = root_path.join("cpu/cpu.cfs_quota_us");
            let period_path = root_path.join("cpu/cpu.cfs_period_us");
            let quota = fs::read_to_string(&quota_path)?.trim().parse::<i64>()?;
            let period = fs::read_to_string(&period_path)?.trim().parse::<u64>()?;
            if quota < 0 {
                return Err(StatReadError::NoCFSQuotaError);
            }
            if period == 0 {
                return Err(StatReadError::InvalidPeriodError(period));
            }
            let limit = quota as f64 / period as f64;
            return Ok(limit);
        }
        Err(StatReadError::UnimplementedCGroupV2Error)
    }

    fn get_clock_cycle() -> u64 {
        unsafe { libc::sysconf(libc::_SC_CLK_TCK) as u64 }
    }

    fn get_system_cpu_usage(&self) -> Result<u64, StatReadError> {
        let stat_file = fs::File::open("/proc/stat")?;
        let reader = io::BufReader::new(stat_file);

        for line in reader.lines() {
            let content = line?;
            if content.starts_with("cpu ") {
                let parts: Vec<&str> = content.split_whitespace().collect();
                if parts.len() < 8 {
                    return Err(StatReadError::InvalidCPUStatError(content));
                }
                let mut total_ticks = 0;
                for part in parts[1..8].iter() {
                    total_ticks += part.parse::<u64>()?;
                }
                let system_usage = (total_ticks * NANO_SECONDS_PER_SECOND) / self.meta.clock_cycle;
                return Ok(system_usage as u64);
            }
        }
        Err(StatReadError::InvalidProcStatError)
    }

    fn get_cgroup_cpu_acct_usage(&self) -> Result<u64, StatReadError> {
        let usage_path = self.root_path.join("cpu/cpuacct.usage");
        let usage = fs::read_to_string(usage_path)?.trim().parse::<u64>()?;
        Ok(usage)
    }

    fn refresh_cpu_stat(&mut self) {
        if self.refresh_index == 0 {
            if let Ok(val) = self.get_cgroup_cpu_acct_usage() {
                self.metrics[0].acct_usage = val;
                self.metrics[1].acct_usage = val;
            }
            if let Ok(val) = self.get_system_cpu_usage() {
                self.metrics[0].system_usage = val;
                self.metrics[1].system_usage = val;
            }
            self.refresh_index += 1;
            return;
        }

        self.metrics[0].acct_usage = self.metrics[1].acct_usage;
        self.metrics[0].system_usage = self.metrics[1].system_usage;
        if let Ok(val) = self.get_cgroup_cpu_acct_usage() {
            self.metrics[1].acct_usage = val;
        }
        if let Ok(val) = self.get_system_cpu_usage() {
            self.metrics[1].system_usage = val;
        }
        self.refresh_index += 1;
    }
}

impl cpu::CPUStatProvider for CGroupCPUStatProvider {
    fn refresh_cpu_stat(&mut self) {
        self.refresh_cpu_stat();
    }

    fn get_cpu_stat(&self) -> cpu::CPUStat {
        let prev = &self.metrics[0];
        let curr = &self.metrics[1];
        if curr.system_usage == prev.system_usage {
            return cpu::CPUStat { usage: 0.0 };
        }
        let usage = ((curr.acct_usage - prev.acct_usage) * self.meta.cores * 100) as f64
            / ((curr.system_usage - prev.system_usage) as f64 * self.meta.quota);
        return cpu::CPUStat {
            usage: usage * 10.0f64,
        };
    }

    fn get_cpu_info(&self) -> cpu::CPUInfo {
        cpu::CPUInfo {
            frequency: self.meta.frequency,
            quota: self.meta.quota,
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum StatReadError {
    #[error("Failed to read file: {0}")]
    IoError(#[from] io::Error),

    #[error("Failed to parse number: {0}")]
    ParseError(#[from] num::ParseIntError),

    #[error("No CFS quota specified")]
    NoCFSQuotaError,

    #[error("Invalid CFS cpu period: {0}")]
    InvalidPeriodError(u64),

    #[error("Invalid CPU frequency")]
    InvalidCPUFrequencyError,

    #[error("Invalid CPU stat format: {0}")]
    InvalidCPUStatError(String),

    #[error("Invalid /proc/stat format, cannot find cpu line")]
    InvalidProcStatError,

    #[error("Unimplemented CGroup V2 error")]
    UnimplementedCGroupV2Error,
}

pub struct LinuxStatReader {}
impl LinuxStatReader {
    fn read_as_u64_array(path: path::PathBuf) -> result::Result<Vec<u64>, StatReadError> {
        let content = fs::read_to_string(&path)?;
        content
            .split_whitespace()
            .map(|num| num.parse::<u64>().map_err(StatReadError::from))
            .collect()
    }

    fn read_as_cpuset_cpus_array(path: path::PathBuf) -> result::Result<Vec<u64>, StatReadError> {
        let content = fs::read_to_string(&path)?.trim().to_string();
        Self::parse_as_cpuset_cpus_array(&content)
    }

    fn parse_as_cpuset_cpus_array(content: &str) -> result::Result<Vec<u64>, StatReadError> {
        let mut cpus = Vec::new();

        for part in content.split(',') {
            if let Some((start, end)) = part.split_once('-') {
                let start: u64 = start.parse()?;
                let end: u64 = end.parse()?;
                cpus.extend(start..=end);
                continue;
            }
            cpus.push(part.parse()?);
        }
        cpus.sort_unstable();
        cpus.dedup();
        Ok(cpus)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_as_cpuset_cpus_array() {
        /*
        // Supported formats:
        // 7
        // 1-6
        // 0,3-4,7,8-10
        // 0-0,0,1-7
        // 03,1-3 <- this is gonna get parsed as [1,2,3]
        // 3,2,1
        // 0-2,3,1
         */
        let cases = vec![
            ("7", vec![7]),
            ("1-6", vec![1, 2, 3, 4, 5, 6]),
            ("0,3-4,7,8-10", vec![0, 3, 4, 7, 8, 9, 10]),
            ("0-0,0,1-7", vec![0, 1, 2, 3, 4, 5, 6, 7]),
            ("03,1-3", vec![1, 2, 3]),
            ("3,2,1", vec![1, 2, 3]),
            ("0-2,3,1", vec![0, 1, 2, 3]),
        ];
        // write test cases here
        for (input, expected) in cases {
            let result = LinuxStatReader::parse_as_cpuset_cpus_array(input);
            assert_eq!(result.unwrap(), expected);
        }
    }

    #[test]
    fn test_cgroup_cpu_stat_provider() {
        use crate::cpu::CPUStatProvider;

        let mut provider =
            super::CGroupCPUStatProvider::new(path::PathBuf::from("/sys/fs/cgroup/"), false)
                .unwrap();
        {
            provider.refresh_cpu_stat();
            std::thread::sleep(std::time::Duration::from_millis(500));
            provider.refresh_cpu_stat();

            for _ in 1..5 {
                let stat = provider.get_cpu_stat();
                assert!(stat.usage > 0.0);
                println!("CPU stat: {:?}", stat);
            }
        }

        {
            let info = provider.get_cpu_info();
            assert!(info.frequency > 0);
            assert!(info.quota > 0.0);
            println!("CPU info: {:?}", info);
        }
    }

    #[tokio::test]
    async fn async_cgroup_cpu_stat_provider() {
        let provider =
            super::CGroupCPUStatProvider::new(path::PathBuf::from("/sys/fs/cgroup/"), false)
                .unwrap();
        let mut loader = crate::cpu::AsyncCPUStatLoader::new(
            Box::new(provider),
            std::time::Duration::from_millis(500),
        );
        loader.start();
        {
            let mut ctr = crate::cpu::CPUStat { usage: 0.0 };
            for _ in 0..5 {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                loader.load_cpu_stat_to(&mut ctr);
                println!("Async CGroup CPU stat: {:?}", ctr);
                assert!(ctr.usage > 0.0);
            }
        }
    }

    #[test]
    fn test_get_clock_cycle() {
        let result = super::CGroupCPUStatProvider::get_clock_cycle();
        println!("Clock cycle: {}", result);
        assert!(result > 0);
    }
}
