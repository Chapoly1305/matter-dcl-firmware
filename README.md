# OTA Firmware Storage (S3-Compatible)

This project exposes exactly two user-facing scripts:

1. `upload.py`
2. `download.py`

All implementation details are internal under `./modules`.

## Requirements

- Python 3.9+
- Dependencies in `requirements.txt`

Install:

```bash
pip install -r requirements.txt
```

## Quick Start (Download First)

Download all mapped files (both networks):

```bash
python download.py --all
```

Download one file by network mapping:

```bash
python download.py --network ota-mainnet --file 5219/1/otaM_xxx.bin
```

Download by SHA256:

```bash
python download.py --hash <sha256>
```

## Download Usage

By mapped file path:

```bash
python download.py --network ota-mainnet --file 5219/1/otaM_xxx.bin
```

By SHA256:

```bash
python download.py --hash <sha256>
```

Restore everything in manifests:

```bash
python download.py --all
python download.py --all --network ota-testnet
```

Backend selection:

```bash
python download.py --all --method auto   # HTTPS first, then S3 fallback
python download.py --all --method https
python download.py --all --method s3
```

## Configuration

### Project Defaults

If you do not set overrides, the scripts use:

- `S3_BUCKET=matter-firmware`
- `S3_ENDPOINT_URL=https://665360de327d92bb87e8e2c2c866c7a1.r2.cloudflarestorage.com`
- `S3_REGION=auto`
- `HTTPS_BASE_URL=https://matter-firmware.chapoly1305.com`

### Download-Related

- `HTTPS_BASE_URL` (only needed if you want to override project default)
- `S3_BUCKET` (only needed if you want to override project default)
- `S3_ENDPOINT_URL` (only needed if you want to override project default)
- `S3_REGION` (only needed if you want to override project default)

### Upload-Related

- `S3_BUCKET` (only needed if you want to override project default)
- `S3_ENDPOINT_URL` (only needed if you want to override project default)

### Common Optional

- `FIRMWARE_SOURCE_ROOT` (default: `./firmware`)
- `CACHE_DIR` (default: `./.cache`)
- `OBJECT_PREFIX` (default: `objects/sha256`)
- `MANIFEST_PREFIX` (default: `manifests`)
- `GLOBAL_MANIFEST_KEY` (default: `manifests/global_manifest.json`)
- `HASH_PROCESSES` (default: `CPU count`)
- `COMPRESS_PROCESSES` (default: `CPU count`)
- `UPLOAD_THREADS` (default: `CPU count`)
- `HEAD_THREADS` (default: `CPU count`)

## What This System Does

- Treats object storage as a content set (`SHA256 -> one stored object`).
- Computes SHA256 on the original file first, then compresses with gzip.
- Uploads each unique SHA256 only once.
- Maintains mapping manifests so downloads can restore all logical file paths, including duplicates.

## Manifest Layout

Stored remotely (authoritative):

- `manifests/global_manifest.json`
- `manifests/ota-mainnet_manifest.json`
- `manifests/ota-testnet_manifest.json`

Cached locally:

- `./.cache/manifests/global_manifest.json`
- `./.cache/manifests/ota-mainnet_manifest.json`
- `./.cache/manifests/ota-testnet_manifest.json`

## Upload Usage

Upload both networks:

```bash
python upload.py
```

Upload one network:

```bash
python upload.py --network ota-mainnet
```

Dry run:

```bash
python upload.py --dry-run
```

## Uploader Credential Setup

### Option A: Use Existing AWS Credential Chain (Recommended)

```bash
python upload.py
```

### Option B: AWS Profile

Put credentials in `~/.aws/credentials` and run:

```bash
export AWS_PROFILE="r2"
# optional overrides if needed:
# export S3_BUCKET="matter-firmware"
# export S3_ENDPOINT_URL="https://665360de327d92bb87e8e2c2c866c7a1.r2.cloudflarestorage.com"
# export S3_REGION="auto"
python upload.py
```

## Security Model

Recommended for upload:

- Use AWS credential chain or profile (preferred).
- Avoid passing secrets in CLI arguments.
- Grant minimum permissions only to required bucket/prefix.

Download can run without credentials when using public HTTPS.

## Notes

- Upload uses S3 SigV4 with payload SHA256 signing enabled.
- Duplicate local files are restored correctly during download because restoration is manifest-driven.
