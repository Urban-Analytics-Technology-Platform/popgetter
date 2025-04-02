use langchain_rust::chain::ChainError;
use polars::error::PolarsError;

#[derive(thiserror::Error, Debug)]
pub enum PopgetterLLMError {
    // When errors are not Send and Sync, can return a generic error
    #[error("Generic error")]
    Generic(#[from] Box<dyn std::error::Error>),
    #[error("Anyhow error")]
    Anyhow(#[from] anyhow::Error),
    #[error("Chain error")]
    ChainError(#[from] ChainError),
    #[error("serde JSON error")]
    SerdeJSONError(#[from] serde_json::Error),
    #[error("polars error")]
    PolarsError(#[from] PolarsError),
}

pub type PopgetterLLMResult<T> = Result<T, PopgetterLLMError>;
