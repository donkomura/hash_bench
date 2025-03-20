use env_logger::Env;

pub fn init_logger() {
    env_logger::Builder::from_env(Env::default().default_filter_or("warn")).init();
}

pub fn init_test_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}
