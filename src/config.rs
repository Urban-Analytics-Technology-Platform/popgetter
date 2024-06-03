#[derive(Debug)]
pub struct Config {
    pub base_path: String,
}

pub fn init() -> Config {
    let default_base_path: String =
        "https://popgetter.blob.core.windows.net/popgetter-dagster-test/test_2".into();
    let base_path = std::env::var("POPGETTER_CLI_BASE_PATH").unwrap_or(default_base_path);

    Config {
        base_path,
    }
}
