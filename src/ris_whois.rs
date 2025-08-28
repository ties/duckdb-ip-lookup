use ipnet::IpNet;
use ipnet_trie::IpnetTrie;
use polars::prelude::*;
use reqwest::Client;
use std::{io::Cursor, net::IpAddr};

const RISWHOIS_V4_URL: &str = "https://www.ris.ripe.net/dumps/riswhoisdump.IPv4.gz";
const RISWHOIS_V6_URL: &str = "https://www.ris.ripe.net/dumps/riswhoisdump.IPv6.gz";

pub(crate) struct LookupTrie<T> {
    trie: IpnetTrie<T>,
}

impl<T> LookupTrie<T> {
    fn new(trie: IpnetTrie<T>) -> Self {
        LookupTrie { trie }
    }

    pub(crate) fn lookup(&self, ip_or_pfx: &str) -> Option<(IpNet, &T)> {
        // Parse the input as either an IP network (with CIDR) or IP address
        let ipnet = match ip_or_pfx {
            s if s.contains('/') => s.parse::<IpNet>().ok(),
            s => s.parse::<IpAddr>().ok().map(IpNet::from),
        };

        match ipnet {
            Some(net) => {
                // Find the longest matching prefix in the trie
                self.trie.longest_match(&net)
            }
            None => None,
        }
    }
}

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

async fn download_and_parse(
    url: &str,
    name: &str,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let client = Client::new();
    let t0 = std::time::Instant::now();
    let data = client.get(url).send().await?.bytes().await?;
    log::info!(
        "Downloaded {} dump in {:.2}s, size: {} bytes",
        name,
        t0.elapsed().as_secs_f64(),
        data.len()
    );

    let df = parse_riswhois_file(&data)?;

    Ok(df)
}

fn build_trie_from_dataframes(
    dfs: Vec<DataFrame>,
) -> Result<LookupTrie<()>, Box<dyn std::error::Error>> {
    let mut table: IpnetTrie<()> = IpnetTrie::new();
    let t0 = std::time::Instant::now();
    for df in dfs {
        let objects = df.take_columns();
        let prefixes = objects[1].str()?.iter();

        for prefix in prefixes.flatten() {
            if let Ok(ip_net) = prefix.parse::<IpNet>() {
                table.insert(ip_net, ());
            }
        }
    }

    log::info!(
        "Built trie with {}v4 + {}v6 entries in {:.2}s",
        table.len().0,
        table.len().1,
        t0.elapsed().as_secs_f64()
    );

    Ok(LookupTrie::new(table))
}

pub fn build_ipnet_trie() -> Result<LookupTrie<()>, Box<dyn std::error::Error>> {
    // Use tokio's block_on to run async code in sync context
    tokio::runtime::Runtime::new()?.block_on(async {
        log::info!("Starting download of IPv4 and IPv6 RIS-Whois dumps.");

        // Download and parse both dumps in parallel
        let (ipv4_result, ipv6_result) = tokio::join!(
            download_and_parse(RISWHOIS_V4_URL, "IPv4"),
            download_and_parse(RISWHOIS_V6_URL, "IPv6")
        );

        // Collect successful results
        let mut dataframes = Vec::new();

        match ipv4_result {
            Ok(df) => dataframes.push(df),
            Err(e) => {
                log::error!("Failed to download/parse IPv4 data: {}", e);
                return Err(e);
            }
        }

        match ipv6_result {
            Ok(df) => dataframes.push(df),
            Err(e) => {
                log::error!("Failed to download/parse IPv6 data: {}", e);
                return Err(e);
            }
        }

        if dataframes.is_empty() {
            return Err("Failed to download any RIS-Whois data".into());
        }

        // Build combined trie from all dataframes
        build_trie_from_dataframes(dataframes)
    })
}
