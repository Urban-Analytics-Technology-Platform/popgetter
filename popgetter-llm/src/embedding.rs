// To run this example execute: cargo run --example vector_store_qdrant --features qdrant

use std::collections::HashMap;

use anyhow::anyhow;
use itertools::{izip, Itertools};
use langchain_rust::{
    schemas::Document,
    vectorstore::{qdrant::Store, VecStoreOptions, VectorStore},
};
use log::info;
use popgetter::{Popgetter, COL};
use rand::{
    rngs::StdRng,
    seq::{IteratorRandom, SliceRandom},
    Rng, SeedableRng,
};
use serde_json::Value;
use tiktoken_rs::cl100k_base;

// Since `.choose_multiple` docs indicates that it does not provide a random sample, this fn
// includes an intermediate vec that is shuffled.
// TODO: explore if can be implemented as a `shuffled()` method through extension trait
fn shuffled_sample_of_size_n_with_skip<T, I: Iterator<Item = T>>(
    iter: I,
    seed: u64,
    sample_n: usize,
    skip: usize,
) -> Vec<T> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut v = iter
        .into_iter()
        .choose_multiple(&mut rng, sample_n)
        .into_iter()
        .collect_vec();
    v.shuffle(&mut rng);
    v.into_iter().skip(skip).collect()
}

pub async fn init_embeddings(
    store: &mut Store,
    sample_n: Option<usize>,
    seed: Option<u64>,
    skip: Option<usize>,
) -> anyhow::Result<()> {
    let popgetter = Popgetter::new_with_config_and_cache(Default::default()).await?;
    let combined_metadata = popgetter
        .metadata
        .combined_metric_source_geometry()
        .0
        .collect()?;
    let mut v = vec![];

    let seed = seed.unwrap_or(StdRng::from_entropy().gen());
    let sample_n = sample_n.unwrap_or(combined_metadata.shape().0);
    let skip = skip.unwrap_or(0);

    // Get shuffled samples
    let human_readable_names = shuffled_sample_of_size_n_with_skip(
        combined_metadata
            .column(COL::METRIC_HUMAN_READABLE_NAME)?
            .str()?
            .into_iter(),
        seed,
        sample_n,
        skip,
    );
    let countries = shuffled_sample_of_size_n_with_skip(
        combined_metadata
            .column(COL::COUNTRY_NAME_SHORT_EN)?
            .str()?
            .into_iter(),
        seed,
        sample_n,
        skip,
    );
    let metric_ids = shuffled_sample_of_size_n_with_skip(
        combined_metadata.column(COL::METRIC_ID)?.str()?.into_iter(),
        seed,
        sample_n,
        skip,
    );
    for (description, country, id) in izip!(human_readable_names, countries, metric_ids) {
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
    let chunk_size = 500;

    // Get tokenizer for tokens:
    // https://platform.openai.com/docs/guides/embeddings/how-can-i-tell-how-many-tokens-a-string-has-before-i-embed-it#how-can-i-tell-how-many-tokens-a-string-has-before-i-embed-it
    let bpe = cl100k_base().unwrap();
    let mut total_tokens: usize = 0;
    for (chunk_idx, docs) in v.chunks(chunk_size).enumerate() {
        total_tokens += docs
            .iter()
            .map(|doc| bpe.encode_ordinary(&doc.page_content).len())
            .sum::<usize>();
        info!(
            "Chunk idx: {chunk_idx:>5};\ttotal documents: {0:>8} (inc. skipped: {1:>8});\ttotal tokens: {2:>12}",
            chunk_size * chunk_idx,
            chunk_size * chunk_idx + skip,
            total_tokens
        );
        store
            .add_documents(docs, &VecStoreOptions::default())
            .await
            // TODO: update error to not convert to string
            .map_err(|err| anyhow!(err.to_string()))?;
    }

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

#[cfg(test)]
mod tests {
    // use super::*;
}
