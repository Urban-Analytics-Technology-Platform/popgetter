use comfy_table::{presets::NOTHING, *};
use itertools::izip;
use log::debug;
use popgetter::{search::SearchResults, COL};

pub fn display_search_results(
    results: SearchResults,
    max_results: Option<usize>,
) -> anyhow::Result<()> {
    let df_to_show = match max_results {
        Some(max) => results.0.head(Some(max)),
        None => results.0,
    };

    for (metric_id, hrn, desc, hxl, date, country, level) in izip!(
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
        df_to_show.column(COL::GEOMETRY_LEVEL)?.rechunk().iter()
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
            ]);

        let column = table.column_mut(0).unwrap();
        column.set_cell_alignment(CellAlignment::Right);

        println!("\n{}", table);
    }
    Ok(())
}
