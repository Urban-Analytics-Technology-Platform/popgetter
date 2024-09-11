use comfy_table::{presets::NOTHING, *};
use itertools::izip;

use polars::{
    df,
    frame::{DataFrame, UniqueKeepStrategy},
    prelude::SortMultipleOptions,
};
use popgetter::{metadata::ExpandedMetadata, search::SearchResults, COL};

pub fn display_countries(countries: DataFrame, max_results: Option<usize>) -> anyhow::Result<()> {
    let df_to_show = match max_results {
        Some(max) => countries.head(Some(max)),
        None => countries,
    };
    let df_to_show = df_to_show.sort([COL::COUNTRY_ID], SortMultipleOptions::default())?;
    let mut table = Table::new();
    table
        .load_preset(NOTHING)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("Country ID").add_attribute(Attribute::Bold),
            Cell::new("Country Name (official)").add_attribute(Attribute::Bold),
            Cell::new("Country Name (short)").add_attribute(Attribute::Bold),
            Cell::new("ISO3116-1 alpha-3").add_attribute(Attribute::Bold),
            Cell::new("ISO3116-2").add_attribute(Attribute::Bold),
        ])
        .set_style(comfy_table::TableComponent::BottomBorder, '─')
        .set_style(comfy_table::TableComponent::MiddleHeaderIntersections, '─')
        .set_style(comfy_table::TableComponent::HeaderLines, '─')
        .set_style(comfy_table::TableComponent::BottomBorderIntersections, '─')
        .set_style(comfy_table::TableComponent::TopBorder, '─')
        .set_style(comfy_table::TableComponent::TopBorderIntersections, '─');
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
    println!("\n{}", table);
    Ok(())
}

pub fn display_search_results(
    results: SearchResults,
    max_results: Option<usize>,
) -> anyhow::Result<()> {
    let df_to_show = match max_results {
        Some(max) => results.0.head(Some(max)),
        None => results.0,
    };

    for (metric_id, hrn, desc, hxl, date, country, level, download_url) in izip!(
        df_to_show.column(COL::METRIC_ID)?.str()?,
        df_to_show.column(COL::METRIC_HUMAN_READABLE_NAME)?.str()?,
        df_to_show.column(COL::METRIC_DESCRIPTION)?.str()?,
        df_to_show.column(COL::METRIC_HXL_TAG)?.str()?,
        df_to_show
            .column(COL::SOURCE_DATA_RELEASE_COLLECTION_PERIOD_START)?
            .rechunk()
            .iter(),
        df_to_show.column(COL::COUNTRY_NAME_SHORT_EN)?.str()?,
        // Note: if using iter on an AnyValue, need to rechunk first.
        df_to_show.column(COL::GEOMETRY_LEVEL)?.rechunk().iter(),
        df_to_show
            .column(COL::METRIC_SOURCE_DOWNLOAD_URL)?
            .rechunk()
            .iter()
    ) {
        let mut table = Table::new();
        table
            .load_preset(NOTHING)
            .set_content_arrangement(ContentArrangement::Dynamic)
            .set_style(comfy_table::TableComponent::BottomBorder, '─')
            .set_style(comfy_table::TableComponent::BottomBorderIntersections, '─')
            .set_style(comfy_table::TableComponent::TopBorder, '─')
            .set_style(comfy_table::TableComponent::TopBorderIntersections, '─')
            .add_row(vec![
                Cell::new("Metric ID").add_attribute(Attribute::Bold),
                metric_id.unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("Metric ID (short)").add_attribute(Attribute::Bold),
                metric_id
                    .unwrap()
                    .chars()
                    .take(8)
                    .collect::<String>()
                    .into(),
            ])
            .add_row(vec![
                Cell::new("Human readable name").add_attribute(Attribute::Bold),
                hrn.unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("Description").add_attribute(Attribute::Bold),
                desc.unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("HXL tag").add_attribute(Attribute::Bold),
                hxl.unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("Collection date").add_attribute(Attribute::Bold),
                format!("{date}").into(),
            ])
            .add_row(vec![
                Cell::new("Country").add_attribute(Attribute::Bold),
                country.unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("Geometry level").add_attribute(Attribute::Bold),
                level.get_str().unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("Source download URL").add_attribute(Attribute::Bold),
                download_url.get_str().unwrap().into(),
            ]);

        let column = table.column_mut(0).unwrap();
        column.set_cell_alignment(CellAlignment::Right);

        println!("\n{}", table);
    }
    Ok(())
}

pub fn display_search_results_no_description(
    results: SearchResults,
    max_results: Option<usize>,
) -> anyhow::Result<()> {
    let df_to_show = match max_results {
        Some(max) => results.0.head(Some(max)),
        None => results.0,
    };

    for (metric_id, hrn, hxl, date, country, level, download_url) in izip!(
        df_to_show.column(COL::METRIC_ID)?.str()?,
        df_to_show.column(COL::METRIC_HUMAN_READABLE_NAME)?.str()?,
        df_to_show.column(COL::METRIC_HXL_TAG)?.str()?,
        df_to_show
            .column(COL::SOURCE_DATA_RELEASE_COLLECTION_PERIOD_START)?
            .rechunk()
            .iter(),
        df_to_show.column(COL::COUNTRY_NAME_SHORT_EN)?.str()?,
        // Note: if using iter on an AnyValue, need to rechunk first.
        df_to_show.column(COL::GEOMETRY_LEVEL)?.rechunk().iter(),
        df_to_show
            .column(COL::METRIC_SOURCE_DOWNLOAD_URL)?
            .rechunk()
            .iter()
    ) {
        let mut table = Table::new();
        table
            .load_preset(NOTHING)
            .set_content_arrangement(ContentArrangement::Dynamic)
            .set_style(comfy_table::TableComponent::BottomBorder, '─')
            .set_style(comfy_table::TableComponent::BottomBorderIntersections, '─')
            .set_style(comfy_table::TableComponent::TopBorder, '─')
            .set_style(comfy_table::TableComponent::TopBorderIntersections, '─')
            .add_row(vec![
                Cell::new("Metric ID").add_attribute(Attribute::Bold),
                metric_id.unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("Metric ID (short)").add_attribute(Attribute::Bold),
                metric_id
                    .unwrap()
                    .chars()
                    .take(8)
                    .collect::<String>()
                    .into(),
            ])
            .add_row(vec![
                Cell::new("Human readable name").add_attribute(Attribute::Bold),
                hrn.unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("HXL tag").add_attribute(Attribute::Bold),
                hxl.unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("Collection date").add_attribute(Attribute::Bold),
                format!("{date}").into(),
            ])
            .add_row(vec![
                Cell::new("Country").add_attribute(Attribute::Bold),
                country.unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("Geometry level").add_attribute(Attribute::Bold),
                level.get_str().unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("Source download URL").add_attribute(Attribute::Bold),
                download_url.get_str().unwrap().into(),
            ]);

        let column = table.column_mut(0).unwrap();
        column.set_cell_alignment(CellAlignment::Right);

        println!("\n{}", table);
    }
    Ok(())
}

pub fn display_summary(results: SearchResults) -> anyhow::Result<()> {
    let df_to_show = results.0;
    let df_to_show = df! {
        COL::METRIC_ID => &[df_to_show.column(COL::METRIC_ID)?.n_unique()? as u64],
        COL::SOURCE_DATA_RELEASE_COLLECTION_PERIOD_START => &[df_to_show
            .column(COL::SOURCE_DATA_RELEASE_COLLECTION_PERIOD_START)?.n_unique()? as u64],
        COL::COUNTRY_NAME_SHORT_EN => &[df_to_show.column(COL::COUNTRY_NAME_SHORT_EN)?.n_unique()? as u64],
        COL::GEOMETRY_LEVEL => &[df_to_show.column(COL::GEOMETRY_LEVEL)?.n_unique()? as u64],
        COL::METRIC_SOURCE_DOWNLOAD_URL => &[df_to_show.column(COL::METRIC_SOURCE_DOWNLOAD_URL)?.n_unique()? as u64]
    }?;
    for (n_metrics, n_dates, n_countries, n_level, n_download_url) in izip!(
        df_to_show.column(COL::METRIC_ID)?.u64()?,
        df_to_show
            .column(COL::SOURCE_DATA_RELEASE_COLLECTION_PERIOD_START)?
            .u64()?,
        df_to_show.column(COL::COUNTRY_NAME_SHORT_EN)?.u64()?,
        // Note: if using iter on an AnyValue, need to rechunk first.
        df_to_show.column(COL::GEOMETRY_LEVEL)?.u64()?,
        df_to_show.column(COL::METRIC_SOURCE_DOWNLOAD_URL)?.u64()?
    ) {
        let mut table = Table::new();
        table
            .load_preset(NOTHING)
            .set_content_arrangement(ContentArrangement::Dynamic)
            .set_style(comfy_table::TableComponent::BottomBorder, '─')
            .set_style(comfy_table::TableComponent::BottomBorderIntersections, '─')
            .set_style(comfy_table::TableComponent::TopBorder, '─')
            .set_style(comfy_table::TableComponent::TopBorderIntersections, '─')
            .add_row(vec![
                Cell::new("Metric ID").add_attribute(Attribute::Bold),
                n_metrics.unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("Collection date").add_attribute(Attribute::Bold),
                n_dates.unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("Country").add_attribute(Attribute::Bold),
                n_countries.unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("Geometry level").add_attribute(Attribute::Bold),
                n_level.unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("Source download URL").add_attribute(Attribute::Bold),
                n_download_url.unwrap().into(),
            ]);

        for col_idx in [0, 1] {
            let column = table.column_mut(col_idx).unwrap();
            column.set_cell_alignment(CellAlignment::Right);
        }

        println!("\n{}", table);
    }
    Ok(())
}

pub fn display_column(results: SearchResults, column: &str) -> anyhow::Result<()> {
    Ok(results
        .0
        .select([column])?
        .iter()
        .for_each(|series| series.rechunk().iter().for_each(|el| println!("{el}"))))
}

pub fn display_metdata_columns(expanded_metadata: &ExpandedMetadata) -> anyhow::Result<()> {
    Ok(expanded_metadata
        .as_df()
        .collect()?
        .get_column_names()
        .into_iter()
        .map(|col| format!("'{col}'"))
        .collect::<Vec<_>>()
        .into_iter()
        .for_each(|val| println!("{val}")))
}

pub fn display_column_unique(results: SearchResults, column: &str) -> anyhow::Result<()> {
    Ok(results
        .0
        .select([column])?
        .unique(None, UniqueKeepStrategy::Any, None)?
        .iter()
        .for_each(|series| series.iter().for_each(|el| println!("{el}"))))
}
