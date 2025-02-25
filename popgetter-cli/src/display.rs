use std::collections::HashMap;
use std::io::Write;
use std::sync::OnceLock;

use comfy_table::{presets::NOTHING, *};
use itertools::izip;
use polars::{frame::DataFrame, prelude::SortMultipleOptions};
use popgetter_core::{metadata::ExpandedMetadata, search::SearchResults, COL};

static LOOKUP: OnceLock<HashMap<&'static str, &'static str>> = OnceLock::new();

// TODO: consider adding to column_names module
fn lookup() -> &'static HashMap<&'static str, &'static str> {
    LOOKUP.get_or_init(|| {
        let mut hm = HashMap::new();
        hm.insert(COL::COUNTRY_ID, "Country ID");
        hm.insert(COL::COUNTRY_NAME_OFFICIAL, "Country Name (official)");
        hm.insert(COL::COUNTRY_NAME_SHORT_EN, "Country Name (short)");
        hm.insert(COL::COUNTRY_ISO3, "ISO3116-1 alpha-3");
        hm.insert(COL::COUNTRY_ISO2, "ISO3116-2");
        hm.insert(COL::METRIC_ID, "Metric ID");
        hm.insert(COL::METRIC_HUMAN_READABLE_NAME, "Human readable name");
        hm.insert(COL::METRIC_DESCRIPTION, "Description");
        hm.insert(COL::METRIC_HXL_TAG, "HXL tag");
        hm.insert(
            COL::SOURCE_DATA_RELEASE_COLLECTION_PERIOD_START,
            "Collection date",
        );
        hm.insert(COL::COUNTRY_NAME_SHORT_EN, "Country");
        hm.insert(COL::GEOMETRY_LEVEL, "Geometry level");
        hm.insert(COL::METRIC_SOURCE_DOWNLOAD_URL, "Source download URL");
        hm
    })
}

fn create_table(width: Option<u16>, header_columns: Option<&[&str]>) -> Table {
    let mut table = Table::new();
    table
        .load_preset(NOTHING)
        .set_style(comfy_table::TableComponent::BottomBorder, '─')
        .set_style(comfy_table::TableComponent::BottomBorderIntersections, '─')
        .set_style(comfy_table::TableComponent::TopBorder, '─')
        .set_style(comfy_table::TableComponent::TopBorderIntersections, '─');

    // Set width if given
    match width {
        Some(width) => {
            table
                .set_width(width)
                .set_content_arrangement(ContentArrangement::DynamicFullWidth);
        }
        None => {
            table.set_content_arrangement(ContentArrangement::Dynamic);
        }
    }

    // Add header if given
    if let Some(columns) = header_columns {
        table
            .set_style(comfy_table::TableComponent::HeaderLines, '─')
            .set_style(comfy_table::TableComponent::MiddleHeaderIntersections, '─')
            .set_header(
                columns
                    .iter()
                    .map(|col| Cell::new(col).add_attribute(Attribute::Bold))
                    .collect::<Vec<_>>(),
            );
    }
    table
}

pub fn display_countries(countries: DataFrame, max_results: Option<usize>) -> anyhow::Result<()> {
    let df_to_show = match max_results {
        Some(max) => countries.head(Some(max)),
        None => countries,
    };
    let df_to_show = df_to_show.sort([COL::COUNTRY_ID], SortMultipleOptions::default())?;
    let mut table = create_table(
        None,
        Some(&[
            lookup().get(COL::COUNTRY_ID).unwrap(),
            lookup().get(COL::COUNTRY_NAME_OFFICIAL).unwrap(),
            lookup().get(COL::COUNTRY_NAME_SHORT_EN).unwrap(),
            lookup().get(COL::COUNTRY_ISO3).unwrap(),
            lookup().get(COL::COUNTRY_ISO2).unwrap(),
        ]),
    );
    for (
        country_id,
        country_name_official,
        country_name_short_en,
        countr_iso3,
        country_iso3116_2,
    ) in izip!(
        df_to_show.column(COL::COUNTRY_ID)?.str()?,
        df_to_show.column(COL::COUNTRY_NAME_OFFICIAL)?.str()?,
        df_to_show.column(COL::COUNTRY_NAME_SHORT_EN)?.str()?,
        df_to_show.column(COL::COUNTRY_ISO3)?.str()?,
        df_to_show.column(COL::COUNTRY_ISO3166_2)?.str()?,
    ) {
        table.add_row(vec![
            country_id.unwrap_or_default(),
            country_name_official.unwrap_or_default(),
            country_name_short_en.unwrap_or_default(),
            countr_iso3.unwrap_or_default(),
            country_iso3116_2.unwrap_or_default(),
        ]);
    }

    Ok(writeln!(&mut std::io::stdout(), "\n{}", table)?)
}

pub fn display_search_results(
    results: SearchResults,
    max_results: Option<usize>,
    exclude_description: bool,
) -> anyhow::Result<()> {
    let mut df_to_show = match max_results {
        Some(max) => results.0.head(Some(max)),
        None => results.0,
    };
    df_to_show.as_single_chunk_par();

    // Set columns conditional on exclude_description arg
    let mut cols = vec![
        COL::METRIC_ID,
        COL::METRIC_HUMAN_READABLE_NAME,
        COL::METRIC_DESCRIPTION,
        COL::METRIC_HXL_TAG,
        COL::SOURCE_DATA_RELEASE_COLLECTION_PERIOD_START,
        COL::COUNTRY_NAME_SHORT_EN,
        COL::GEOMETRY_LEVEL,
        COL::METRIC_SOURCE_DOWNLOAD_URL,
    ];
    if exclude_description {
        cols.retain(|&col| col.ne(COL::METRIC_DESCRIPTION));
    }
    // See example for iteration over SeriesIter: https://stackoverflow.com/a/72443329
    let mut iters = df_to_show
        .columns(&cols)?
        .iter()
        .map(|s| s.iter())
        .collect::<Vec<_>>();

    for _ in 0..df_to_show.height() {
        let mut table = create_table(Some(100), None);
        for (iter, col) in iters.iter_mut().zip(cols.to_vec()) {
            let value = iter.next().unwrap();
            match col {
                // Format: metric ID
                COL::METRIC_ID => {
                    table
                        .add_row(vec![
                            Cell::new(lookup().get(col).unwrap()).add_attribute(Attribute::Bold),
                            value.clone().get_str().unwrap().into(),
                        ])
                        .add_row(vec![
                            Cell::new("Metric ID (short)").add_attribute(Attribute::Bold),
                            value
                                .get_str()
                                .unwrap()
                                .chars()
                                .take(8)
                                .collect::<String>()
                                .into(),
                        ]);
                }
                // Format: str
                COL::COUNTRY_NAME_SHORT_EN
                | COL::METRIC_HUMAN_READABLE_NAME
                | COL::METRIC_DESCRIPTION
                | COL::METRIC_HXL_TAG
                | COL::GEOMETRY_LEVEL
                | COL::METRIC_SOURCE_DOWNLOAD_URL => {
                    table.add_row(vec![
                        Cell::new(lookup().get(col).unwrap()).add_attribute(Attribute::Bold),
                        value.get_str().unwrap().into(),
                    ]);
                }
                // Format: dates
                COL::SOURCE_DATA_RELEASE_COLLECTION_PERIOD_START => {
                    table.add_row(vec![
                        Cell::new(lookup().get(col).unwrap()).add_attribute(Attribute::Bold),
                        format!("{value}").into(),
                    ]);
                }
                // No missing columns are possible since all matching should be include in columns
                _ => {
                    unreachable!()
                }
            }
        }
        writeln!(&mut std::io::stdout(), "{}", table)?;
    }
    Ok(())
}

pub fn display_summary(results: SearchResults) -> anyhow::Result<()> {
    let df_to_show = results.0;
    // Columns to summarise
    let cols = [
        COL::METRIC_ID,
        COL::SOURCE_DATA_RELEASE_COLLECTION_PERIOD_START,
        COL::COUNTRY_NAME_SHORT_EN,
        COL::GEOMETRY_LEVEL,
        COL::METRIC_SOURCE_DOWNLOAD_URL,
    ];
    // Get unique values of each columns
    let n_uniques = cols
        .iter()
        .map(|col| df_to_show.column(col).and_then(|el| el.n_unique()))
        .collect::<Result<Vec<usize>, _>>()?;

    // Create table
    let mut table = create_table(None, Some(&["Column", "Unique values"]));

    // Write values
    for (col, n_unique) in cols.iter().copied().zip(n_uniques.into_iter()) {
        table.add_row(vec![
            Cell::new(col).add_attribute(Attribute::Bold),
            n_unique.into(),
        ]);
    }
    // Alignment
    let column = table.column_mut(1).unwrap();
    column.set_cell_alignment(CellAlignment::Right);

    Ok(writeln!(&mut std::io::stdout(), "\n{}", table)?)
}

/// Display a given column from the search results
pub fn display_column(search_results: SearchResults, column: &str) -> anyhow::Result<()> {
    Ok(search_results
        .0
        .column(column)?
        .rechunk()
        .iter()
        .map(|el| el.get_str().map(|s| s.to_string()).unwrap())
        .try_for_each(|el| writeln!(&mut std::io::stdout(), "{el}"))?)
}

/// Display the unique values of a given column from the search results
pub fn display_column_unique(search_results: SearchResults, column: &str) -> anyhow::Result<()> {
    Ok(search_results
        .0
        .column(column)?
        .unique()?
        .iter()
        .map(|el| el.get_str().map(|s| s.to_string()).unwrap())
        .try_for_each(|el| writeln!(&mut std::io::stdout(), "{el}"))?)
}

/// Display the columns of the expanded metadata that can be used for displaying metrics results
/// (either whole column or unique results)
pub fn display_metdata_columns(expanded_metadata: &ExpandedMetadata) -> anyhow::Result<()> {
    Ok(expanded_metadata
        .as_df()
        .collect()?
        .get_column_names()
        .into_iter()
        .try_for_each(|el| writeln!(&mut std::io::stdout(), "{el}"))?)
}
