#!/usr/bin/env python3
"""
Fast S3 scan: find .txt objects where any *data* line has policy number in cols 6–23.
- Skips first and last line (metadata) without loading whole file (stream + lag buffer).
- Parallelizes S3 GETs (biggest speedup).
- Early-exits per object as soon as a match is found.
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import boto3
import botocore
from botocore.config import Config

# Policy number is ONLY the 18-character field at positions 6–23 (1-indexed).
# E.g. in "155396H1096926..." the policy number is "6H1096926" (positions 6–23), not "155396H1096926".
POLICY_SLICE = slice(5, 23)  # positions 6–23 inclusive (1-indexed)

def normalize_policy(s: str) -> str:
    return s.strip().replace(" ", "")

def line_matches_policy(line: str, want: str) -> bool:
    if len(line) < POLICY_SLICE.stop:
        return False
    field = line[POLICY_SLICE]
    return normalize_policy(field) == want

def object_contains_policy_streaming(s3, bucket: str, key: str, want: str) -> bool:
    """
    Stream object line-by-line. Skip first and last line (metadata) using lag buffer:
      - discard first line
      - keep one previous line; only evaluate it once you read the next line
      - never evaluate the final buffered line => last line skipped
    """
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"]

        # Read first line and discard (metadata)
        it = body.iter_lines(chunk_size=256 * 1024)  # larger chunks = fewer round trips
        try:
            next(it)  # discard first line
        except StopIteration:
            return False  # empty file

        prev: Optional[str] = None

        for raw in it:
            # raw is bytes
            line = raw.decode("utf-8", errors="replace")

            # lag-buffer logic: evaluate prev now that we know it's not the last line
            if prev is not None:
                if line_matches_policy(prev, want):
                    return True

            prev = line

        # Do NOT evaluate prev here (it is the last line, metadata)
        return False

    except botocore.exceptions.ClientError:
        return False
    except Exception:
        # Optional: log if you want visibility
        return False

def iter_txt_keys(s3, bucket: str, prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            key = obj["Key"]
            if key.endswith("/") or not key.lower().endswith(".txt"):
                continue
            yield key

def main() -> None:
    bucket = input("S3 bucket: ").strip()
    if not bucket:
        print("Bucket is required.")
        return

    prefix = input("S3 prefix (folder path, or leave empty): ").strip()
    policy_number = input("Policy number: ").strip()

    want = normalize_policy(policy_number)
    if not want:
        print("Policy number is required.")
        return

    if prefix and not prefix.endswith("/"):
        prefix += "/"

    #tune these based on your needs, increasing the pool size or worker will increase the speed of the scan, but also increase the memory usage 
    max_workers = int(os.environ.get("S3_SCAN_WORKERS", "32"))
    max_pool = int(os.environ.get("S3_MAX_POOL", "64"))

    profile = os.environ.get("AWS_PROFILE")
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"

    session_kwargs = {"region_name": region}
    if profile:
        session_kwargs["profile_name"] = profile

    session = boto3.Session(**session_kwargs)

    #Bigger connection pool helps when using threads
    s3 = session.client(
        "s3",
        config=Config(
            max_pool_connections=max_pool,
            retries={"max_attempts": 10, "mode": "adaptive"},
        ),
    )

    print(f"\nSearching bucket={bucket!r} prefix={prefix!r} for policy {want!r}...\n")

    matching: list[str] = []
    scanned = 0

    #keep the number of in-flight futures bounded so memory doesn't grow with huge prefixes
    inflight_limit = max_workers * 4
    futures = {}

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for key in iter_txt_keys(s3, bucket, prefix):
            scanned += 1
            fut = ex.submit(object_contains_policy_streaming, s3, bucket, key, want)
            futures[fut] = key

            if len(futures) >= inflight_limit:
                for done in as_completed(list(futures.keys())[:max_workers]):
                    k = futures.pop(done)
                    try:
                        if done.result():
                            matching.append(k)
                            print(f"  Match: {k}")
                    except Exception:
                        pass #treat worker error as no match

        #drain remaining
        for done in as_completed(futures):
            k = futures.pop(done)
            try:
                if done.result():
                    matching.append(k)
                    print(f"  Match: {k}")
            except Exception:
                pass

    print(f"\nScanned {scanned} .txt objects.")
    if matching:
        print(f"Found {len(matching)} file(s) containing policy {want!r} in positions 6–23:")
        for k in matching:
            print(k)
    else:
        print(f"No files found with policy {want!r} in positions 6–23.")

if __name__ == "__main__":
    main()
