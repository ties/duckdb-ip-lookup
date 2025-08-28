# DuckDB plugin for riswhois lookups

This is a very rough work in progress.

```
$ make configure
$ RUST_BACKTRACE=1 make release && RUST_BACKTRACE=1 duckdb -unsigned
...[build]...
D LOAD './build/release/extension/ip_more_less_specific//ip_more_less_specific.duckdb_extension';
2025-08-28T08:23:13.334339Z  INFO ip_more_less_specific::lib::ris_whois: Starting download of IPv4 and IPv6 RIS-Whois dumps.
2025-08-28T08:24:11.909939Z  INFO ip_more_less_specific::lib::ris_whois: Downloaded IPv4 dump in 58.56s, size: 5200744 bytes
2025-08-28T08:24:20.841727Z  INFO ip_more_less_specific::lib::ris_whois: Downloaded IPv6 dump in 67.49s, size: 1182698 bytes
2025-08-28T08:24:21.157642Z  INFO ip_more_less_specific::lib::ris_whois: Built trie with 1183850v4 + 280089v6 entries in 0.28s
100% ▕████████████████████████████████████████████████████████████
D select riswhois_longest_prefix('1.1.1.1');
┌────────────────────────────────────┐
│ riswhois_longest_prefix('1.1.1.1') │
│              varchar               │
├────────────────────────────────────┤
│ 1.1.1.0/24                         │
└────────────────────────────────────┘
```


## Changelog

  * Evaluated usage of s3-fifo: performance was lower _with_ s3fifo for all fifo sizes (16/128/1024) after storing String.
    * Performance degradation between ~27-34% with s3fifo compared to baseline [0] - but rows are distinct.
  * Stored `String` values in trie.
  * Criterion benchmarking harness for core lookup code.
  * Removed heavy (polars) dependencies, dropping file size significantly.
  * Added a cache for frequently seen items
    * before LRU/s3-fifo: ~221 cpu-seconds for 1.456.072.651 rows
    * after: s3-fifo@128: ~190 cpu-seconds for 1.456.072.651 rows

[0]: Data on the OpenIntel Radar dataset on a AMD Ryzen 7205P:

| Benchmark | Mean Time | Change |
|-----------|-----------|--------|
| original order | 864.80 ms | +28.805% |
| random order | 893.81 ms | +27.066% |
| alphabetical order | 710.77 ms | +33.986% |
