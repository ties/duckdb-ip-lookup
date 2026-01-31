use flate2::read::GzDecoder;
use ipnet::IpNet;
use ipnet_trie::IpnetTrie;
use reqwest::Client;
use std::{
    io::{BufRead, BufReader, Cursor},
    net::IpAddr,
};

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
            s if s[ip_or_pfx.len() - 4..].contains('/') => s.parse::<IpNet>().ok(),
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

fn parse_riswhois_file(data: &[u8]) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let cursor = Cursor::new(data);
    let decoder = GzDecoder::new(cursor);
    let reader = BufReader::new(decoder);

    let mut prefixes = Vec::new();
    let mut lines_skipped = 0;

    for line in reader.lines() {
        let line = line?;

        // Skip first 17 lines
        if lines_skipped < 17 {
            lines_skipped += 1;
            continue;
        }

        // Parse tab-separated values and extract the prefix (second column)
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() >= 2 {
            prefixes.push(parts[1].to_string());
        }
    }

    Ok(prefixes)
}

async fn download_and_parse(
    url: &str,
    name: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let client = Client::new();
    let t0 = std::time::Instant::now();
    let data = client.get(url).send().await?.bytes().await?;
    log::info!(
        "Downloaded {} dump in {:.2}s, size: {} bytes",
        name,
        t0.elapsed().as_secs_f64(),
        data.len()
    );

    let prefixes = parse_riswhois_file(&data)?;

    Ok(prefixes)
}

fn build_trie_from_prefixes(
    prefixes: impl Iterator<Item = String>,
) -> Result<LookupTrie<()>, Box<dyn std::error::Error>> {
    let mut table: IpnetTrie<()> = IpnetTrie::new();
    let t0 = std::time::Instant::now();

    for prefix in prefixes {
        if let Ok(ip_net) = prefix.parse::<IpNet>() {
            table.insert(ip_net, ());
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

        // Extract results and chain them using itertools
        let combined_prefixes = match (ipv4_result, ipv6_result) {
            (Ok(v4), Ok(v6)) => v4.into_iter().chain(v6),
            (Err(e), _) => {
                log::error!("Failed to download/parse IPv4 data: {}", e);
                return Err(e);
            }
            (_, Err(e)) => {
                log::error!("Failed to download/parse IPv6 data: {}", e);
                return Err(e);
            }
        };

        // Build combined trie from chained iterator
        build_trie_from_prefixes(combined_prefixes)
    })
}
