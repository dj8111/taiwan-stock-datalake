#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
年度打包與外部雲端移轉 (archive_storage.py)
每年針對指定年度 (預設上一年) 進行數據壓縮封存、產生 SHA256，並上傳至 Cloudflare R2 / AWS S3。
"""

import os
import sys
import tarfile
import hashlib
import logging
import argparse
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def create_archive(archive_year: str, output_tar: str):
    logging.info(f"Scanning data directories for year {archive_year}...")
    matched_folders = []
    
    # 遍歷 data 目錄，找出所有名稱為 archive_year 的年份目錄
    if os.path.exists("data"):
        for root, dirs, files in os.walk("data"):
            if os.path.basename(root) == str(archive_year):
                matched_folders.append(root)

    # 包含 manifests
    manifest_year_dir = os.path.join("manifests", str(archive_year))
    if os.path.exists(manifest_year_dir):
        matched_folders.append(manifest_year_dir)

    if not matched_folders:
        logging.warning(f"No directories found for year {archive_year}.")
        return False

    logging.info(f"Found {len(matched_folders)} directories to archive. Writing to {output_tar}...")
    with tarfile.open(output_tar, "w:gz") as tar:
        for folder in matched_folders:
            logging.info(f"  Adding: {folder}")
            tar.add(folder, arcname=folder)

    tar_size_mb = os.path.getsize(output_tar) / (1024 * 1024)
    logging.info(f"Archive successfully created: {output_tar} ({tar_size_mb:.2f} MB)")
    return True

def create_checksum(tar_file: str, sha_file: str):
    logging.info(f"Calculating SHA256 checksum for {tar_file}...")
    sha = hashlib.sha256()
    with open(tar_file, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            sha.update(chunk)
    digest = sha.hexdigest()
    with open(sha_file, "w", encoding="utf-8") as f:
        f.write(f"{digest}  {os.path.basename(tar_file)}\n")
    logging.info(f"SHA256: {digest} saved to {sha_file}")
    return digest

def upload_to_cloud(files_to_upload: list, archive_year: str):
    """
    支援 Cloudflare R2 (S3-compatible) 或標準 AWS S3
    """
    try:
        import boto3
        from botocore.config import Config
    except ImportError:
        logging.error("boto3 is not installed. Skipping cloud upload.")
        return False

    r2_account = os.getenv("R2_ACCOUNT_ID")
    access_key = os.getenv("R2_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("R2_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
    bucket = os.getenv("R2_BUCKET_NAME") or os.getenv("S3_BUCKET_NAME")

    if not all([access_key, secret_key, bucket]):
        logging.info("Cloud storage credentials not fully configured. Skipping remote upload.")
        return False

    # 若有 R2_ACCOUNT_ID 則走 Cloudflare R2 端點，否則走標準 AWS S3
    if r2_account:
        endpoint_url = f"https://{r2_account}.r2.cloudflarestorage.com"
        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4")
        )
        provider_name = "Cloudflare R2"
    else:
        aws_region = os.getenv("AWS_REGION", "ap-northeast-1")
        client = boto3.client(
            "s3",
            region_name=aws_region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        provider_name = f"AWS S3 ({aws_region})"

    logging.info(f"Connected to {provider_name}. Uploading files to bucket: {bucket}...")
    for f in files_to_upload:
        if os.path.exists(f):
            dest_key = f"archives/{archive_year}/{os.path.basename(f)}"
            logging.info(f"Uploading {f} -> s3://{bucket}/{dest_key}")
            client.upload_file(f, bucket, dest_key)
            logging.info(f"  [OK] Uploaded {dest_key}")

    logging.info("Cloud storage upload completed.")
    return True

if __name__ == "__main__":
    default_year = str(datetime.now().year - 1)
    parser = argparse.ArgumentParser(description="Annual Archive & S3/R2 Sync")
    parser.add_argument("--year", type=str, default=os.getenv("ARCHIVE_YEAR", default_year), help="Year to archive (default: previous year)")
    parser.add_argument("--skip-upload", action="store_true", help="Skip cloud upload")
    args = parser.parse_args()

    year = str(args.year)
    tar_filename = f"taiwan_stock_{year}.tar.gz"
    sha_filename = f"{tar_filename}.sha256"

    if create_archive(year, tar_filename):
        create_checksum(tar_filename, sha_filename)
        if not args.skip_upload:
            upload_to_cloud([tar_filename, sha_filename], year)
    else:
        logging.info("Nothing to archive.")
        sys.exit(0)

