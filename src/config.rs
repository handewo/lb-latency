use serde::Deserialize;
use std::io::Read;
use std::path::Path;

#[derive(Deserialize)]
pub struct Config {
    pub check_interval: u64,
    pub check_timeout: u64,
    pub proxys: Vec<ProxyCfg>,
}

#[derive(Deserialize)]
pub struct ProxyCfg {
    pub frontend: u16,
    pub backend_addrs: Vec<String>,
}

pub fn load_config(path: String) -> Config {
    let path = Path::new(&path);
    let mut f = std::fs::File::open(path).unwrap();
    let mut cfg = String::new();
    f.read_to_string(&mut cfg).unwrap();
    toml::from_str(cfg.as_str()).unwrap()
}
