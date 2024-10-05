use bon::Builder;

use enum_dispatch::enum_dispatch;

use polars::error::PolarsResult;
use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

#[enum_dispatch(Transform)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PopgetterTransform {
    Category(CategoryTransform),
    Census(CensusTransform),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryTransform {
    include_metadata: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder, Default)]
pub struct CensusTransform {
    rename_column: Option<(String, String)>,
}

pub trait Transform {
    fn transform(&self, output: DataFrame) -> PolarsResult<DataFrame>;
}

impl Transform for CategoryTransform {
    fn transform(&self, output: DataFrame) -> PolarsResult<DataFrame> {
        todo!()
    }
}

impl Transform for CensusTransform {
    fn transform(&self, mut output: DataFrame) -> PolarsResult<DataFrame> {
        if let Some((old_name, new_name)) = self.rename_column.as_ref() {
            output.rename(old_name, new_name)?;
        }
        Ok(output)
    }
}
