#[cfg(feature = "llm")]
use langchain_rust::chain::ChainError;
use polars::error::PolarsError;
use popgetter_core::error::PopgetterError;
#[cfg(feature = "llm")]
use popgetter_llm::error::PopgetterLLMError;

#[derive(thiserror::Error, Debug)]
pub enum PopgetterCliError {
    // When errors are not Send and Sync, can return a generic error
    #[error("Generic error")]
    Generic(#[from] Box<dyn std::error::Error>),
    #[error("Anyhow error")]
    Anyhow(#[from] anyhow::Error),
    #[cfg(feature = "llm")]
    #[error("Chain error")]
    ChainError(#[from] ChainError),
    #[error("serde JSON error")]
    SerdeJSONError(#[from] serde_json::Error),
    #[error("polars error")]
    PolarsError(#[from] PolarsError),
    #[error("popgetter error")]
    PopgetterError(#[from] PopgetterError),
    #[cfg(feature = "llm")]
    #[error("popgetter error")]
    PopgetterLLMError(#[from] PopgetterLLMError),
    #[error("std IO error")]
    IOError(#[from] std::io::Error),
}

pub type PopgetterCliResult<T> = Result<T, PopgetterCliError>;
