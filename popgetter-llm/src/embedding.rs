// To run this example execute: cargo run --example vector_store_qdrant --features qdrant

use std::collections::HashMap;

use anyhow::anyhow;
use itertools::izip;
use langchain_rust::{
    schemas::Document,
    vectorstore::{qdrant::Store, VecStoreOptions, VectorStore},
};
use popgetter::{Popgetter, COL};
use rand::{rngs::StdRng, seq::IteratorRandom, SeedableRng};
use serde_json::Value;

pub async fn init_embeddings(store: &mut Store) -> anyhow::Result<()> {
    let popgetter = Popgetter::new_with_config_and_cache(Default::default()).await?;
    let combined_metadata = popgetter
        .metadata
        .combined_metric_source_geometry()
        .0
        .collect()?;
    let mut v = vec![];

    for (description, country, id) in izip!(
        combined_metadata
            .column(COL::METRIC_HUMAN_READABLE_NAME)?
            .str()?
            .into_iter()
            // TODO: make sample configurable
            .choose_multiple(&mut StdRng::seed_from_u64(0), 1000),
        combined_metadata
            .column(COL::COUNTRY_NAME_SHORT_EN)?
            .str()?
            .into_iter()
            .choose_multiple(&mut StdRng::seed_from_u64(0), 1000),
        combined_metadata
            .column(COL::METRIC_ID)?
            .str()?
            .into_iter()
            .choose_multiple(&mut StdRng::seed_from_u64(0), 1000)
    ) {
        let s: String = description.ok_or(anyhow!("Not a str"))?.into();

        // TODO: add method to return HashMap of a row with keys (columns) and values
        // Could just use the IDs and lookup in polars too.
        let mut hm: HashMap<String, Value> = HashMap::new();
        hm.insert(
            "country".to_owned(),
            Value::String(country.unwrap().to_string()),
        );
        hm.insert(
            COL::METRIC_ID.to_owned(),
            Value::String(id.unwrap().to_string()),
        );

        // TODO: add other metadata
        let doc = Document::new(s).with_metadata(hm);
        v.push(doc);
    }

    // TODO: add rate limiting
    // Add documents to store
    store
        .add_documents(&v, &VecStoreOptions::default())
        .await
        // TODO: update error to not convert to string
        .map_err(|err| anyhow!(err.to_string()))?;

    Ok(())
}

pub async fn query_embeddings(
    query: &str,
    limit: usize,
    store: &Store,
) -> anyhow::Result<Vec<Document>> {
    // TODO: see if we can subset similarity search by metadata values
    let results = store
        .similarity_search(query, limit, &VecStoreOptions::default())
        .await
        // TODO: fix error type
        .unwrap();
    Ok(results)
}
