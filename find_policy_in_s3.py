#!/usr/bin/env python3
"""
Fast S3 scan: find .txt objects where any *data* line has policy number(s) in cols 6–23.
- Accepts a single policy number or comma-separated list.
- Skips first and last line (metadata) without loading whole file (stream + lag buffer).
- Parallelizes S3 GETs. One pass per file checks all requested policies.
- Prints each file as it is searched; final output grouped by policy number.
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

import boto3
import botocore
from botocore.config import Config

POLICY_SLICE = slice(5, 23) 

def normalize_policy(s: str) -> str:
    return s.strip().replace(" ", "")

def policy_in_line(line: str) -> Optional[str]:
    """Return normalized policy from line (cols 6–23) or None."""
    if len(line) < POLICY_SLICE.stop:
        return None
    return normalize_policy(line[POLICY_SLICE])

def object_matching_policies_streaming(
    s3, bucket: str, key: str, want_set: set[str]
) -> set[str]:
    """
    Stream object line-by-line. Skip first and last line (metadata).
    Return the set of wanted policy numbers that appear in any data line.
    """
    print(f"  Searching: {key}", flush=True)
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"]

        it = body.iter_lines(chunk_size=256 * 1024)
        try:
            next(it)  # discard first line
        except StopIteration:
            return set()

        found: set[str] = set()
        prev: Optional[str] = None

        for raw in it:
            line = raw.decode("utf-8", errors="replace")

            if prev is not None:
                p = policy_in_line(prev)
                if p is not None and p in want_set:
                    found.add(p)

            prev = line

        return found

    except botocore.exceptions.ClientError:
        return set()
    except Exception:
        return set()

def iter_txt_keys(s3, bucket: str, prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            key = obj["Key"]
            if key.endswith("/") or not key.lower().endswith(".txt"):
                continue
            yield key

def parse_policy_input(raw: str) -> list[str]:
    """Parse single policy or comma/newline-separated list; return ordered, deduped list."""
    parts = raw.replace("\n", ",").split(",")
    seen: set[str] = set()
    out: list[str] = []
    for p in parts:
        norm = normalize_policy(p.strip())
        if norm and norm not in seen:
            seen.add(norm)
            out.append(norm)
    return out

def main() -> None:
    bucket = input("S3 bucket: ").strip()
    if not bucket:
        print("Bucket is required.")
        return

    prefix = input("S3 prefix (folder path, or leave empty): ").strip()
    policy_input = input(
        "Policy number(s): single value, or comma-separated list: "
    ).strip()

    policy_list = parse_policy_input(policy_input)
    if not policy_list:
        print("At least one policy number is required.")
        return

    want_set = set(policy_list)
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    max_workers = int(os.environ.get("S3_SCAN_WORKERS", "32"))
    max_pool = int(os.environ.get("S3_MAX_POOL", "64"))

    profile = os.environ.get("AWS_PROFILE")
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"

    session_kwargs = {"region_name": region}
    if profile:
        session_kwargs["profile_name"] = profile

    session = boto3.Session(**session_kwargs)

    s3 = session.client(
        "s3",
        config=Config(
            max_pool_connections=max_pool,
            retries={"max_attempts": 10, "mode": "adaptive"},
        ),
    )

    print(
        f"\nSearching bucket={bucket!r} prefix={prefix!r} for {len(policy_list)} policy(s)...\n"
    )

    results_by_policy: dict[str, list[str]] = {p: [] for p in policy_list}
    scanned = 0
    inflight_limit = max_workers * 4
    futures = {}

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for key in iter_txt_keys(s3, bucket, prefix):
            scanned += 1
            fut = ex.submit(object_matching_policies_streaming, s3, bucket, key, want_set)
            futures[fut] = key

            if len(futures) >= inflight_limit:
                for done in as_completed(list(futures.keys())[:max_workers]):
                    k = futures.pop(done)
                    try:
                        for pol in done.result():
                            results_by_policy[pol].append(k)
                    except Exception:
                        pass

        for done in as_completed(futures):
            k = futures.pop(done)
            try:
                for pol in done.result():
                    results_by_policy[pol].append(k)
            except Exception:
                pass

    print(f"\nScanned {scanned} .txt objects.\n")
    print("--- Results by policy ---")

    report_lines = [
        f"S3 policy scan report — {datetime.now().isoformat(timespec='seconds')}",
        f"Bucket: {bucket!r}  Prefix: {prefix!r}",
        f"Policies: {', '.join(policy_list)}",
        f"Scanned {scanned} .txt objects.",
        "",
        "--- Results by policy ---",
    ]
    for pol in policy_list:
        files = results_by_policy[pol]
        print(f"\nPolicy {pol}:")
        report_lines.append(f"\nPolicy {pol}:")
        if files:
            for f in files:
                print(f"  {f}")
                report_lines.append(f"  {f}")
            print(f"  ({len(files)} file(s))")
            report_lines.append(f"  ({len(files)} file(s))")
        else:
            print("  (no files found)")
            report_lines.append("  (no files found)")

    report_text = "\n".join(report_lines)
    out_dir = Path.home() / "Downloads"
    if not out_dir.is_dir():
        out_dir = Path.cwd()
    out_path = out_dir / f"policy_scan_report_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt"
    try:
        out_path.write_text(report_text, encoding="utf-8")
        print(f"\nReport saved: {out_path}")
    except OSError:
        fallback = Path.cwd() / out_path.name
        fallback.write_text(report_text, encoding="utf-8")
        print(f"\nReport saved (Downloads unavailable): {fallback}")

if __name__ == "__main__":
    main()
