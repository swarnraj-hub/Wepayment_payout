import csv
import os
import re
import sys
import time
from datetime import datetime, timedelta

import boto3
import requests
from botocore.exceptions import BotoCoreError, ClientError


# ═══════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════
merchant_ids = [
    582, 585, 587, 780, 858, 841, 586, 785, 877, 969,
    801, 859, 803, 832, 938, 948, 953, 970, 978,
]

TOKEN    = os.getenv("WEPOUT_TOKEN", "")
currency = "BRL"
BASE_URL = "https://api.wepayout.com.br/v1/payout/payments"

DEFAULT_TIMEOUT            = 60
LONG_TIMEOUT_MERCHANTS     = {585: 180}
MAX_RETRIES                = 6
RETRYABLE_STATUS_CODES     = {429, 500, 502, 503, 504}
NON_RETRYABLE_STATUS_CODES = {401, 403}

# S3 config — use `or` so empty-string secrets from GitHub Actions fall back to defaults
S3_BUCKET             = os.getenv("S3_BUCKET", "")          or ""
S3_PREFIX             = os.getenv("S3_PREFIX", "")          or "wepayments/payout/raw_daily"
S3_KEY                = os.getenv("S3_KEY", "")             or ""
AWS_REGION            = os.getenv("AWS_REGION", "")         or ""
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID", "")  or ""
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "") or ""

_AWS_REGION_RE = re.compile(r"^[a-z]+-[a-z]+-[0-9]+$")


# ═══════════════════════════════════════════════════════════
# STARTUP VALIDATION
# ═══════════════════════════════════════════════════════════
def validate_config():
    """Fail fast with a clear message if the API token is missing."""
    if not TOKEN:
        print("\n" + "=" * 60)
        print("CONFIGURATION ERROR — script cannot run:")
        print("  WEPOUT_TOKEN is not set.")
        print("  Fix: GitHub repo -> Settings -> Secrets -> Actions")
        print("       add WEPOUT_TOKEN with your WEpayout API token.")
        print("=" * 60 + "\n")
        raise SystemExit(1)


def validate_s3_config():
    """Returns a list of problem strings; empty list means all good."""
    issues = []

    missing = [name for name, val in [
        ("AWS_ACCESS_KEY_ID",     AWS_ACCESS_KEY_ID),
        ("AWS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY),
        ("AWS_REGION",            AWS_REGION),
        ("S3_BUCKET",             S3_BUCKET),
    ] if not val]
    if missing:
        issues.append(
            "Missing GitHub secrets: " + ", ".join(missing) + "\n"
            "    Fix: Settings -> Secrets -> Actions -> add each one."
        )

    if AWS_REGION and not _AWS_REGION_RE.match(AWS_REGION.strip()):
        issues.append(
            f"AWS_REGION='{AWS_REGION}' is not valid.\n"
            "    Expected format: ap-southeast-1 / us-east-1 / eu-west-2\n"
            "    Common mistakes: underscores instead of hyphens, extra spaces, quotes."
        )

    return issues


# ═══════════════════════════════════════════════════════════
# DATE RANGE & FLAGS
# ═══════════════════════════════════════════════════════════
def get_date_range():
    """Priority: CLI args -> env vars -> last 7 days."""
    if len(sys.argv) >= 3:
        return sys.argv[1], sys.argv[2]
    start = os.getenv("START_DATE")
    end   = os.getenv("END_DATE")
    if start and end:
        return start, end
    today = datetime.now().date()
    return (today - timedelta(days=7)).strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")


def should_upload_to_s3():
    if len(sys.argv) >= 4:
        return sys.argv[3].strip().lower() == "true"
    return os.getenv("UPLOAD_S3", "true").strip().lower() == "true"


# ═══════════════════════════════════════════════════════════
# HEADERS  (built after token is validated)
# ═══════════════════════════════════════════════════════════
def build_headers():
    return {
        "Authorization": f"Bearer {TOKEN}",
        "Accept":        "application/json",
        "Content-Type":  "application/json",
        "User-Agent":    "Mozilla/5.0",
    }


# ═══════════════════════════════════════════════════════════
# PARSING HELPERS
# ═══════════════════════════════════════════════════════════
def extract_payments(payload):
    if isinstance(payload, dict):
        if isinstance(payload.get("data"), list):
            return payload["data"]
        if isinstance(payload.get("payments"), list):
            return payload["payments"]
    if isinstance(payload, list):
        return payload
    return []


def extract_last_page(payload):
    if not isinstance(payload, dict):
        return None
    meta = payload.get("meta") or payload.get("pagination") or payload.get("page")
    if isinstance(meta, dict):
        for key in ("last_page", "total_pages", "pages"):
            value = meta.get(key)
            if isinstance(value, int) and value > 0:
                return value
    return None


def parse_api_datetime(value):
    if not value:
        return ""
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S.%fZ"):
            try:
                return datetime.strptime(value, fmt).strftime("%d-%m-%Y")
            except ValueError:
                pass
        return value[:10]
    if hasattr(value, "strftime"):
        return value.strftime("%d-%m-%Y")
    return str(value)


def normalize_amount(value):
    if value in (None, ""):
        return ""
    text = str(value).strip()
    for prefix in ("R$", "$", "BRL"):
        text = text.replace(prefix, "")
    text = text.replace(" ", "")
    if text.count(",") == 1 and text.count(".") > 1:
        text = text.replace(".", "").replace(",", ".")
    if "," in text and "." in text:
        text = text.replace(",", "")
    return text.strip()


# ═══════════════════════════════════════════════════════════
# CSV
# ═══════════════════════════════════════════════════════════
CSV_PRIMARY_FIELDS = [
    "Merchant", "WE ID", "Invoice", "Status", "SubStatus", "Created Date",
    "Beneficiary", "Beneficiary Document", "Beneficiary Pix Key",
    "Beneficiary Bank Code", "Beneficiary Branch", "Beneficiary Branch Digit",
    "Beneficiary Account", "Beneficiary Account Digit", "Beneficiary Account Type",
    "Amount", "Payment Type", "Currency Charged", "Source Currency",
    "Source Amount", "Processed Amount", "Updated Date", "Description",
    "Authentication", "Rejected Reason",
    "Payment Originator Legal Entity Name", "Payment Originator Website",
]
CSV_MAPPED_KEYS = set(CSV_PRIMARY_FIELDS)


def flatten_record(record):
    flat = {}
    for key, value in record.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                flat[f"{key}_{sub_key}"] = sub_value
        else:
            flat[key] = value
    return flat


def build_mapped_row(record):
    merchant    = record.get("merchant")    or {}
    beneficiary = record.get("beneficiary") or {}
    status      = record.get("status")      or {}
    amount           = normalize_amount(record.get("amount"))
    source_amount    = normalize_amount(record.get("source_amount"))
    processed_amount = source_amount or amount

    return {
        "Merchant":                             merchant.get("name", ""),
        "WE ID":                                record.get("id", ""),
        "Invoice":                              record.get("custom_code", ""),
        "Status":                               status.get("name", ""),
        "SubStatus":                            record.get("sub_status", "") or record.get("substatus", ""),
        "Created Date":                         parse_api_datetime(record.get("created_at")),
        "Beneficiary":                          beneficiary.get("name", ""),
        "Beneficiary Document":                 beneficiary.get("document", ""),
        "Beneficiary Pix Key":                  beneficiary.get("pix_key", ""),
        "Beneficiary Bank Code":                beneficiary.get("bank_code", ""),
        "Beneficiary Branch":                   beneficiary.get("bank_branch", ""),
        "Beneficiary Branch Digit":             beneficiary.get("bank_branch_digit", ""),
        "Beneficiary Account":                  beneficiary.get("account", ""),
        "Beneficiary Account Digit":            beneficiary.get("account_digit", ""),
        "Beneficiary Account Type":             beneficiary.get("account_type", ""),
        "Amount":                               amount,
        "Payment Type":                         record.get("payment_type", ""),
        "Currency Charged":                     record.get("currency", ""),
        "Source Currency":                      record.get("source_currency", ""),
        "Source Amount":                        source_amount,
        "Processed Amount":                     processed_amount,
        "Updated Date":                         parse_api_datetime(record.get("updated_at")),
        "Description":                          record.get("description", "") or "",
        "Authentication":                       record.get("authentication_code", ""),
        "Rejected Reason":                      record.get("rejection_description", ""),
        "Payment Originator Legal Entity Name": "",
        "Payment Originator Website":           "",
    }


def save_csv(data, filename):
    """Always writes the file (even header-only) so S3 upload never hits FileNotFoundError."""
    rows        = []
    extra_keys  = []
    seen_extras = set()

    for record in (data or []):
        if not isinstance(record, dict):
            continue
        flat   = flatten_record(record)
        mapped = build_mapped_row(record)
        extras = {k: v for k, v in flat.items() if k not in CSV_MAPPED_KEYS}
        for k in extras:
            if k not in seen_extras:
                seen_extras.add(k)
                extra_keys.append(k)
        rows.append({**mapped, **extras})

    fieldnames = CSV_PRIMARY_FIELDS + extra_keys
    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    if rows:
        print(f"  CSV written: {filename}  ({len(rows)} rows, {len(fieldnames)} columns)")
    else:
        print(f"  CSV written: {filename}  (headers only — no data fetched)")


# ═══════════════════════════════════════════════════════════
# S3 UPLOAD
# ═══════════════════════════════════════════════════════════
def upload_csv_to_s3(local_path, date_str):
    issues = validate_s3_config()
    if issues:
        print("\n" + "=" * 60)
        print("S3 UPLOAD SKIPPED — fix these issues:")
        for issue in issues:
            print(f"  x {issue}")
        print("=" * 60)
        return False

    abs_path = os.path.abspath(local_path)
    if not os.path.exists(abs_path):
        print(f"\nS3 upload SKIPPED — file not found: {abs_path}")
        return False

    region = AWS_REGION.strip()

    # ✅ Your S3 path logic
    s3_key = f"wepayments/payout/raw_daily/payout-export-{date_str}.csv"

    # ✅ ADD THIS LINE HERE
    print(f"FINAL S3 PATH → s3://{S3_BUCKET}/{s3_key}")

    print(f"\nUploading to s3://{S3_BUCKET}/{s3_key}  (region: {region}) ...")

    try:
        s3 = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        s3.upload_file(abs_path, S3_BUCKET, s3_key)
        print(f"  Upload complete: s3://{S3_BUCKET}/{s3_key}")
        return True

    except ClientError as e:
        code = e.response["Error"]["Code"]
        msg  = e.response["Error"]["Message"]
        print(f"\nS3 upload FAILED [{code}]: {msg}")
        print("Checklist:")
        print(f"  - Does bucket '{S3_BUCKET}' exist in region '{region}'?")
        print(f"  - IAM user has s3:PutObject on arn:aws:s3:::{S3_BUCKET}/* ?")
        print(f"  - Are AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY correct?")
        raise SystemExit(1)

    except BotoCoreError as e:
        print(f"\nS3 upload FAILED (BotoCoreError): {e}")
        raise SystemExit(1)


# ═══════════════════════════════════════════════════════════
# API FETCH
# ═══════════════════════════════════════════════════════════
def get_timeout_for_merchant(merchant_id):
    return LONG_TIMEOUT_MERCHANTS.get(merchant_id, DEFAULT_TIMEOUT)


def iter_date_windows(start_str, end_str):
    current = datetime.strptime(start_str, "%Y-%m-%d").date()
    end     = datetime.strptime(end_str,   "%Y-%m-%d").date()
    while current <= end:
        day = current.strftime("%Y-%m-%d")
        yield day, day
        current += timedelta(days=1)


def fetch_page_with_retry(params, merchant_id, hdrs):
    timeout = get_timeout_for_merchant(merchant_id)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(BASE_URL, headers=hdrs, params=params, timeout=timeout)

            if response.status_code == 200:
                try:
                    return response.json(), None
                except ValueError as e:
                    return None, f"Invalid JSON for merchant {merchant_id}: {e}"

            if response.status_code in NON_RETRYABLE_STATUS_CODES:
                return None, (
                    f"HTTP {response.status_code} AUTH_ERROR merchant {merchant_id} — "
                    f"check WEPOUT_TOKEN. Body: {response.text[:300]}"
                )

            if response.status_code in RETRYABLE_STATUS_CODES:
                wait = min(2 ** (attempt - 1), 8)
                print(f"  HTTP {response.status_code} merchant {merchant_id} attempt {attempt}/{MAX_RETRIES}, retry in {wait}s...")
                if attempt < MAX_RETRIES:
                    time.sleep(wait)
                    continue
                return None, f"HTTP {response.status_code} after {MAX_RETRIES} retries for merchant {merchant_id}"

            return None, f"HTTP {response.status_code} merchant {merchant_id} | {response.text[:300]}"

        except requests.exceptions.Timeout:
            wait = min(2 ** (attempt - 1), 8)
            print(f"  Timeout merchant {merchant_id} attempt {attempt}/{MAX_RETRIES}, retry in {wait}s...")
            if attempt < MAX_RETRIES:
                time.sleep(wait)
                continue
            return None, f"Timed out merchant {merchant_id} after {MAX_RETRIES} attempts"

        except requests.exceptions.RequestException as e:
            wait = min(2 ** (attempt - 1), 8)
            print(f"  Request error merchant {merchant_id} attempt {attempt}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(wait)
                continue
            return None, f"Request failed merchant {merchant_id}: {e}"

    return None, f"Request failed merchant {merchant_id} after {MAX_RETRIES} retries"


def fetch_range(merchant_id, range_start, range_end, hdrs):
    page     = 1
    per_page = 100
    all_data = []

    while True:
        print(f"  Merchant {merchant_id} | {range_start} -> {range_end} | page {page}")
        params = {
            "merchant_id":    merchant_id,
            "currency":       currency,
            "created_after":  range_start,
            "created_before": range_end,
            "page":           page,
            "per_page":       per_page,
            "order_by":       "id",
            "sort":           "asc",
        }
        result, error = fetch_page_with_retry(params, merchant_id, hdrs)
        if error:
            return None, error

        payments = extract_payments(result)
        if not payments:
            break

        all_data.extend(payments)
        print(f"    -> {len(payments)} records (total so far: {len(all_data)})")

        last_page = extract_last_page(result)
        if last_page is not None:
            if page >= last_page:
                break
        else:
            if len(payments) < per_page:
                break

        page += 1
        time.sleep(0.3)

    return all_data, None


def discover_merchant_ids(hdrs, sample_start, sample_end):
    """Try the API without merchant_id to find which merchants are accessible."""
    print("\n[Discovery] Calling API without merchant_id filter...")
    params = {
        "currency":       currency,
        "created_after":  sample_start,
        "created_before": sample_end,
        "page":           1,
        "per_page":       100,
    }
    try:
        resp = requests.get(BASE_URL, headers=hdrs, params=params, timeout=DEFAULT_TIMEOUT)
        print(f"[Discovery] HTTP {resp.status_code}  Body: {resp.text[:500]}")
        if resp.status_code == 200:
            payments = extract_payments(resp.json())
            ids = list(dict.fromkeys(
                p.get("merchant", {}).get("id") or p.get("merchant_id")
                for p in payments
                if p.get("merchant", {}).get("id") or p.get("merchant_id")
            ))
            print(f"[Discovery] {len(payments)} records found. Merchant IDs: {ids}")
            return ids if ids else None
    except Exception as exc:
        print(f"[Discovery] Exception: {exc}")
    return None


def fetch_merchant(merchant_id, start, end, hdrs):
    data, error = fetch_range(merchant_id, start, end, hdrs)

    if error is None:
        return data

    # Auth error — never recoverable
    if "AUTH_ERROR" in (error or ""):
        print(f"  x Merchant {merchant_id}: {error}")
        return []

    # Other error — try day-by-day
    print(f"  ! Merchant {merchant_id}: broad range failed -> falling back to daily chunks")
    fallback = []
    for rs, re_ in iter_date_windows(start, end):
        daily, daily_err = fetch_range(merchant_id, rs, re_, hdrs)
        if daily_err:
            if "AUTH_ERROR" in daily_err:
                print(f"  x Merchant {merchant_id}: auth error on {rs} — stopping daily fallback.")
                break
            print(f"  ! Merchant {merchant_id} {rs}: {daily_err}")
            continue
        fallback.extend(daily)
    return fallback


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════
if __name__ == "__main__":

    # 1. Validate token — exit immediately with clear message if missing
    validate_config()

    start_date, end_date = get_date_range()
    upload_flag = should_upload_to_s3()

    print("=" * 60)
    print(f"Date range : {start_date} -> {end_date}")
    print(f"Merchants  : {len(merchant_ids)}")
    print(f"Upload S3  : {upload_flag}")
    print("=" * 60)

    hdrs = build_headers()

    # 2. Fetch
    all_data        = []
    merchant_counts = {}

    for mid in merchant_ids:
        print(f"\n-- Merchant {mid} --")
        records = fetch_merchant(mid, start_date, end_date, hdrs)
        merchant_counts[mid] = len(records)
        all_data.extend(records)

    # 3. Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for mid in merchant_ids:
        count = merchant_counts.get(mid, 0)
        note  = "" if count > 0 else "  <- no data / permission denied"
        print(f"  Merchant {mid:>4}: {count:>6} records{note}")
    print(f"\n  TOTAL: {len(all_data)} records  ({start_date} to {end_date})")
    print("=" * 60)

    # 4. Auto-discovery if zero records from hardcoded IDs
    if len(all_data) == 0:
        print("\n[Discovery] Zero records from hardcoded IDs. Probing API without merchant_id...")
        disc = discover_merchant_ids(hdrs, start_date, end_date)
        if disc:
            print(f"\n[Discovery] Retrying with IDs from API: {disc}")
            for mid in disc:
                print(f"\n-- Merchant {mid} (discovered) --")
                recs = fetch_merchant(mid, start_date, end_date, hdrs)
                merchant_counts[mid] = len(recs)
                all_data.extend(recs)
            print(f"\n[Discovery] TOTAL after retry: {len(all_data)} records")
        else:
            print(
                "\n  Auto-discovery returned nothing.\n"
                "  Check your WEpayout dashboard for the correct merchant IDs\n"
                "  and update merchant_ids = [...] at the top of Wepayment.py"
            )

    # 5. Write CSV (always, even if empty)
    csv_path = os.path.abspath("payments_full_data.csv")
    print(f"\nWriting CSV -> {csv_path}")
    save_csv(all_data, csv_path)

    # 6. S3 upload
    if upload_flag:
        upload_csv_to_s3(csv_path, end_date)
    else:
        print("\nS3 upload skipped (upload_s3=false).")
