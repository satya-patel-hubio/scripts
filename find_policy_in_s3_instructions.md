# Find Policy in S3 – Usage & Documentation

## Overview

**Script:** `find_policy_in_s3.py`

Searches an S3 bucket (and optional prefix) for `.txt` files that contain a given **policy number** in **character positions 6–23** of any data line. Intended for WCPOLS-style policy documents where:

- **Line 1** and the **last line** are metadata (skipped).
- **Middle lines** are data; the policy number is in columns 6–23 (spaces are ignored when matching).

The script lists objects under the bucket/prefix, streams each `.txt` file line-by-line, and returns every object key where at least one data line has that policy number in positions 6–23.

---

## Prerequisites

- **Python 3** (3.8+)
- **boto3** (`pip install boto3`)
- **AWS access** to the target S3 bucket (LIST + GET). Use AWS SSO or credentials as below.

---

## AWS Setup (SSO)

1. **Log in with AWS SSO** (use your assigned profile name):
   ```bash
   aws sso login --profile YOUR_PROFILE_NAME
   ```

2. **Set the profile** for the current shell (optional if you use default profile):
   ```bash
   export AWS_PROFILE=YOUR_PROFILE_NAME
   ```

3. **Optional – export credentials** (alternative to profile; useful in some environments):
   ```bash
   eval "$(aws configure export-credentials --profile YOUR_PROFILE_NAME --format env)"
   export AWS_REGION=us-east-1
   export AWS_DEFAULT_REGION=us-east-1
   ```

The script uses whatever is in effect: `AWS_PROFILE`, or exported credentials, or the default profile.

---

## How to Run

```bash
python3 find_policy_in_s3.py
```

The script will **prompt** for:

| Prompt            | Description                                                                 | Example                          |
|-------------------|-----------------------------------------------------------------------------|----------------------------------|
| **S3 bucket**     | Bucket name (required)                                                      | `emc-prod-regul8`                |
| **S3 prefix**     | Folder path under the bucket; leave empty to search the whole bucket       | `file-gen/wcpols/archive` or `WCSTAT` |
| **Policy number** | Policy to find (must be the value in positions 6–23 only); spaces are stripped | `6H1096926`  |

---

## Output

- **While running:** Each matching object key is printed as `Match: s3://bucket/prefix/.../file.txt` (key only, no `s3://` prefix in script output).
- **At the end:**
  - Total number of `.txt` objects scanned.
  - List of all object keys that contain the policy number in positions 6–23 (or a “no files found” message).

---

## Examples

**Search under a specific prefix:**
```
S3 bucket: emc-prod-regul8
S3 prefix (folder path, or leave empty): file-gen/wcpols/archive
Policy number: 6H1096926
```

**Search under WCSTAT:**
```
S3 bucket: emc-prod-regul8
S3 prefix (folder path, or leave empty): WCSTAT
Policy number: 6H1096926
```

**Search entire bucket** (use only if the bucket is small and you intend to scan all keys):
```
S3 bucket: emc-prod-regul8
S3 prefix (folder path, or leave empty): [press Enter]
Policy number: 6H1096926
```

---

## S3 Path Reference

| You want to search…                         | Bucket             | Prefix                |
|--------------------------------------------|--------------------|------------------------|
| `s3://emc-prod-regul8/file-gen/wcpols/archive` | `emc-prod-regul8`  | `file-gen/wcpols/archive` |
| `s3://emc-prod-regul8/WCSTAT/`             | `emc-prod-regul8`  | `WCSTAT`               |

The script searches the given prefix and **all keys under it** (recursive). No need to add a trailing `/`; the script adds it if missing.


## AWS Resources & Cost

- **S3 LIST:** One request per 1,000 keys listed under the bucket/prefix.
- **S3 GET:** One request per `.txt` object scanned (full object is streamed).
- **Data transfer:** You pay for egress for data downloaded from S3 (e.g. to your laptop). Running from EC2 in the same region as the bucket can reduce or avoid egress cost.

Use a **narrow prefix** when possible to limit the number of objects listed and downloaded.

---

## Troubleshooting

| Issue | What to do |
|-------|------------|
| **“No such module ‘boto3’”** | Run `pip install boto3` (or `pip3 install boto3`). |
| **AWS credential / access errors** | Ensure you’ve run `aws sso login --profile YOUR_PROFILE` and, if needed, `export AWS_PROFILE=YOUR_PROFILE`. |
| **No matches** | Confirm the policy number (no extra characters), bucket name, and prefix. Check that the policy document format uses positions 6–23 for the policy number and that the file is `.txt`. |
| **Script slow** | Use a more specific prefix; optionally increase `S3_SCAN_WORKERS` and `S3_MAX_POOL` (within your network/machine limits). |

---

## File Format Assumption

The script assumes WCPOLS-style fixed-format text:

- **First line:** metadata (skipped).
- **Last line:** metadata (skipped).
- **All lines in between:** data lines; the policy number is **only** the 18-character field at **positions 6–23** (1-based). E.g. if a line starts with `155396H1096926...`, the policy number is `6H1096926` (cols 6–23), not the longer prefix. Spaces in that range are stripped when matching.

If your documents use a different layout, the script would need to be adjusted (e.g. different column range or delimiter).
