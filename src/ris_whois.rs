use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use ipnet_trie::IpnetTrie;
use itertools::multizip;
use polars::prelude::*;
use reqwest::blocking::Client;
use std::{
    io::Cursor,
    net::{Ipv4Addr, Ipv6Addr},
};

fn parse_riswhois_file(data: &[u8]) -> Result<DataFrame, PolarsError> {
    let cursor = Cursor::new(data);
    CsvReadOptions::default()
        .into_reader_with_file_handle(cursor)
        .with_options(
            CsvReadOptions::default()
                .with_parse_options(CsvParseOptions::default().with_separator(b'\t'))
                .with_has_header(false)
                .with_skip_lines(17)
                .with_infer_schema_length(Some(100))
                .with_schema(Some(Arc::new(Schema::from_iter([
                    ("origin_as".into(), DataType::String), // Can be an AS-set
                    ("prefix".into(), DataType::String),
                    ("visibility".into(), DataType::UInt32),
                ])))),
        )
        .finish()
}

fn build_ipnet_trie() -> Result<IpnetTrie<Origin>, Box<dyn std::error::Error>> {
    let data = Client::new().get(RISWHOIS_URL).send()?.bytes()?;
    log::info!("Data loaded, size: {} bytes", data.len());

    let df = parse_riswhois_file(&data)?;

    let objects = df.take_columns();
    let origin_as_ = objects[0].str()?.iter();
    let prefix_ = objects[1].str()?.iter();
    let visibility_ = objects[2].u32()?.iter();

    let mut table: IpnetTrie<()> = IpnetTrie::new();

    let i_len = origin_as_.len();
    log::info!(
        "Beginning to populate the IPnetTrie ({} entries)... 🚧",
        i_len
    );

    for (origin_as, prefix, visibility) in izip!((origin_as_, prefix_, visibility_)) {
        if let (Some(origin_as), Some(prefix), Some(visibility)) = (origin_as, prefix, visibility) {
            if let Ok(ip_net) = prefix.parse::<IpNet>() {
                table.insert(ip_net, ());
            }
        }
    }
    log::info!("Memory usage: {}b", get_memory_usage());

    Ok(table)
}