use clap::Parser;

#[cfg(debug_assertions)]
static BASE_DIR:&str = "./rusts3-data-debug";

#[cfg(not(debug_assertions))]
static BASE_DIR:&str = "./rusts3-data";

#[cfg(debug_assertions)]
static PORT:i32 = 8001;

#[cfg(not(debug_assertions))]
static PORT:i32 = 8000;

#[derive(Parser, Debug, Clone)]
pub struct CliArg {
    #[arg(short, long, default_value_t = String::from(BASE_DIR))]
    base_dir: String,

    #[arg(long, default_value_t = String::from("0.0.0.0"), help="Bind IP address")]
    bind_address: String,

    #[arg(long, default_value_t = PORT, help="Bind port number")]
    bind_port: i32,

    #[arg(long, default_value_t = String::from(""), help="Log4rs config file")]
    log_conf:String
}

impl CliArg {
    pub fn bind_port(&self) -> i32 {
        self.bind_port
    }

    pub fn bind_address(&self) -> &str {
        &self.bind_address
    }

    pub fn base_dir(&self) -> &str {
        &self.base_dir
    }

    pub fn log4rs_config_file(&self) ->&str {
        &self.log_conf
    }
}