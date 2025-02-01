pub mod cpu;

#[cfg(all(target_os = "linux", feature = "cgroup"))]
pub mod cgroup;

#[cfg(test)]
mod tests {}
