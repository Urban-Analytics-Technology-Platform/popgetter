//! Error types.

#[derive(thiserror::Error, Debug)]
pub enum PopgetterError {
    #[error("Wrapped anyhow error: {0}")]
    AnyhowError(#[from] anyhow::Error),
    #[error("Connection failure.")]
    FailedConnection,
    #[error("Metric not found.")]
    MetricNotFound(String),
    #[error("Invalid search syntax: {0}")]
    InvalidSearchQuery(String),
    #[error("Non-existent geometry for metric requested: {0}")]
    NonExistentGeometry(String),
    #[error("Wrapped polars error: {0}")]
    PolarsError(#[from] polars::error::PolarsError),
    #[error("Unknown error.")]
    Unknown,
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;

    use super::*;

    #[test]
    fn test_anyhow() {
        let anyhow_error = anyhow!("An anyhow error");
        let popgetter_error: PopgetterError = anyhow_error.into();
        println!("{}", popgetter_error);
    }
}
