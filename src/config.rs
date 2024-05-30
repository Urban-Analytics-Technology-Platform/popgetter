#[derive(Debug)]
pub struct Config {
    pub base_path: String,
    http_client: reqwest::Client,
}

pub fn init() -> Config {
    let default_base_path: String =
        "https://popgetter.blob.core.windows.net/popgetter-dagster-test/test_2".into();
    let base_path = std::env::var("POPGETTER_CLI_BASE_PATH").unwrap_or(default_base_path);
    let http_client = reqwest::Client::new();

    Config {
        base_path,
        http_client,
    }
}

impl Config {
    // TODO hacky
    fn base_path_is_internet(&self) -> bool {
        self.base_path.starts_with("http")
    }

    // TODO use PopgetterError instead of anyhow
    // Read in a text file either using the local filesystem or http
    pub async fn get_text_file(&self, rel_path: &str) -> Result<String, anyhow::Error> {
        let full_path = format!("{}/{}", self.base_path, rel_path);
        if self.base_path_is_internet() {
            self.http_client
                .get(&full_path)
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to fetch file '{}': {}", &full_path, e))?
                .text()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to read file '{}': {}", &full_path, e))
        } else {
            std::fs::read_to_string(&full_path)
                .map_err(|e| anyhow::anyhow!("Failed to read file '{}': {}", &full_path, e))
        }
    }
}
