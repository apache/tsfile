# Datasets for Chapter 6 Experiments

Sources from TsFile VLDB paper (Table 1).

## Dataset Profile

| Dataset | Points | Series | Devices | Tags | Fields | Source |
|---------|--------|--------|---------|------|--------|--------|
| REDD    | 56M    | 115    | 115     | building, meter | power (DOUBLE) | Public |
| GeoLife | 72M    | 543    | 181     | user_id | lat, lon, alt (DOUBLE) | Public |
| TDrive  | 18M    | 17778  | 8889    | taxi_id | lon, lat (DOUBLE) | Public |
| TSBS    | 496M   | 16000  | 4000    | name, fleet, driver | lat, lon, ele, vel (DOUBLE) | Generated |

## Quick Start

```bash
# 1. Download raw data (see below for URLs)
# 2. Prepare each dataset
python3 prepare_redd.py    --raw-dir ./raw/redd    --out-dir ./prepared/redd
python3 prepare_geolife.py --raw-dir ./raw/geolife --out-dir ./prepared/geolife
python3 prepare_tdrive.py  --raw-dir ./raw/tdrive  --out-dir ./prepared/tdrive
bash    prepare_tsbs.sh     --out-dir ./prepared/tsbs

# 3. Or prepare all at once
bash prepare_all.sh
```

## Data Sources

### REDD (Reference Energy Disaggregation Data Set)
- Paper: [Kolter & Johnson 2011]
- URL: http://redd.csail.mit.edu/ (requires registration)
- Download `low_freq.tar.bz2`, extract to `raw/redd/`
- Expected structure: `raw/redd/house_{1..6}/channel_{1..N}.dat`

### GeoLife
- Paper: [Zheng et al. 2010]
- URL: https://download.microsoft.com/download/F/4/8/F4894AA5-FDBC-481E-9285-D5F8C4C4F039/Geolife%20Trajectories%201.3.zip
- Extract to `raw/geolife/`
- Expected structure: `raw/geolife/Data/{000..181}/Trajectory/*.plt`

### TDrive
- Paper: [Yuan et al. 2010, 2011]
- URL: https://www.microsoft.com/en-us/research/publication/t-drive-driving-directions-based-on-taxi-trajectories/
- Download both parts, extract to `raw/tdrive/`
- Expected structure: `raw/tdrive/{1..10357}.txt`

### TSBS (Time Series Benchmark Suite)
- Repo: https://github.com/timescale/tsbs
- IoT use case (trucks with coordinates, velocity)
- Generated via `tsbs_generate_data`, no manual download needed
- Requires Go toolchain

## Prepared Output Format

Each `prepare_*.py` produces a sorted CSV:

```
timestamp,tag1[,tag2,...],field1[,field2,...]
```

- Sorted by device_id (tag combination), then timestamp
- Timestamp: epoch seconds (int64)
- Tags: string
- Fields: double

Plus a `meta.json` with schema and statistics.
