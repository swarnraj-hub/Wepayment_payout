import csv
import json
import os
import time
from datetime import datetime, timedelta

import requests


# ---------------- CONFIG ---------------- #
merchant_ids = [
    582, 585, 587, 780, 858, 841, 586, 785, 877, 969,
    801, 859, 803, 832, 938, 948, 953, 970, 978,
]

# Prefer an environment variable so the token is not hard-coded.
TOKEN = os.getenv("WEPOUT_TOKEN", "PASTE_YOUR_TOKEN_HERE")
currency = "BRL"

# List endpoint documented by WEpayments payout API.
BASE_URL = "https://api.wepayout.com.br/v1/payout/payments"

# ---------------- DATE RANGE ---------------- #
# Use local date boundaries and keep the range inclusive.
start_date = "2026-04-08"
end_date = "2026-04-15"
# today = datetime.now().date()
# past_date = today - timedelta(days=7)

# start_date = past_date.strftime("%Y-%m-%d")
# end_date = today.strftime("%Y-%m-%d")

print(f"\nDate Filter Applied: {start_date} to {end_date}\n")


# ---------------- HEADERS ---------------- #
headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json",
}

DEFAULT_TIMEOUT = 60
LONG_TIMEOUT_MERCHANTS = {
    585: 180,
}
MAX_RETRIES = 6
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

CSV_FIELDS = [
    "Merchant",
    "WE ID",
    "Invoice",
    "Status",
    "SubStatus",
    "Created Date",
    "Beneficiary",
    "Beneficiary Document",
    "Beneficiary Pix Key",
    "Beneficiary Bank Code",
    "Beneficiary Branch",
    "Beneficiary Branch Digit",
    "Beneficiary Account",
    "Beneficiary Account Digit",
    "Beneficiary Account Type",
    "Amount",
    "Payment Type",
    "Currency Charged",
    "Source Currency",
    "Source Amount",
    "Processed Amount",
    "Updated Date",
    "Description",
    "Authentication",
    "Rejected Reason",
    "Payment Originator Legal Entity Name",
    "Payment Originator Website",
]


def extract_payments(payload):
    """
    Normalize the API response into a payments list.
    The API may return:
      - a dict with a `data` key
      - a dict with a `payments` key
      - a raw list
    """
    if isinstance(payload, dict):
        if isinstance(payload.get("data"), list):
            return payload["data"]
        if isinstance(payload.get("payments"), list):
            return payload["payments"]
    if isinstance(payload, list):
        return payload
    return []


def extract_last_page(payload):
    """
    Try to detect the last page from common pagination shapes.
    If not available, return None and we will fall back to item counts.
    """
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
    text = text.replace(".", "").replace(",", ".") if text.count(",") == 1 and text.count(".") > 1 else text
    # If the value looked like 1,141.68, remove thousands separators but keep the decimal point.
    if "," in text and "." in text:
        text = text.replace(",", "")
    return text.strip()


def build_csv_row(payment):
    merchant = payment.get("merchant") or {}
    beneficiary = payment.get("beneficiary") or {}
    status = payment.get("status") or {}

    amount = normalize_amount(payment.get("amount"))
    source_amount = normalize_amount(payment.get("source_amount"))
    processed_amount = source_amount or amount

    return {
        "Merchant": merchant.get("name", ""),
        "WE ID": payment.get("id", ""),
        "Invoice": payment.get("custom_code", ""),
        "Status": status.get("name", ""),
        "SubStatus": payment.get("sub_status", "") or payment.get("substatus", ""),
        "Created Date": parse_api_datetime(payment.get("created_at")),
        "Beneficiary": beneficiary.get("name", ""),
        "Beneficiary Document": beneficiary.get("document", ""),
        "Beneficiary Pix Key": beneficiary.get("pix_key", ""),
        "Beneficiary Bank Code": beneficiary.get("bank_code", ""),
        "Beneficiary Branch": beneficiary.get("bank_branch", ""),
        "Beneficiary Branch Digit": beneficiary.get("bank_branch_digit", ""),
        "Beneficiary Account": beneficiary.get("account", ""),
        "Beneficiary Account Digit": beneficiary.get("account_digit", ""),
        "Beneficiary Account Type": beneficiary.get("account_type", ""),
        "Amount": amount,
        "Payment Type": payment.get("payment_type", ""),
        "Currency Charged": payment.get("currency", ""),
        "Source Currency": payment.get("source_currency", ""),
        "Source Amount": source_amount,
        "Processed Amount": processed_amount,
        "Updated Date": parse_api_datetime(payment.get("updated_at")),
        "Description": payment.get("description", "") or "",
        "Authentication": payment.get("authentication_code", ""),
        "Rejected Reason": payment.get("rejection_description", ""),
        "Payment Originator Legal Entity Name": "",
        "Payment Originator Website": "",
    }


def save_csv(data, filename="payments_full_data.csv"):
    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for payment in data:
            if isinstance(payment, dict):
                writer.writerow(build_csv_row(payment))


def get_timeout_for_merchant(merchant_id):
    return LONG_TIMEOUT_MERCHANTS.get(merchant_id, DEFAULT_TIMEOUT)


def iter_date_windows(start_date_str, end_date_str):
    """
    Yield inclusive one-day windows as (start, end) strings.
    This is used as a fallback when a broad merchant/date query is too slow.
    """
    current = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    while current <= end:
        day = current.strftime("%Y-%m-%d")
        yield day, day
        current += timedelta(days=1)


def fetch_page_with_retry(params, merchant_id):
    """
    Retry transient failures like timeouts and 5xx responses.
    Returns (response_json, None) on success or (None, error_message) on failure.
    """
    timeout = get_timeout_for_merchant(merchant_id)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                BASE_URL,
                headers=headers,
                params=params,
                timeout=timeout,
            )

            if response.status_code == 200:
                try:
                    return response.json(), None
                except ValueError as e:
                    return None, f"Invalid JSON for merchant {merchant_id}: {e}"

            if response.status_code in RETRYABLE_STATUS_CODES:
                wait_seconds = min(2 ** (attempt - 1), 8)
                print(
                    f"Retryable HTTP {response.status_code} for merchant {merchant_id} "
                    f"(attempt {attempt}/{MAX_RETRIES}). Waiting {wait_seconds}s..."
                )
                if attempt < MAX_RETRIES:
                    time.sleep(wait_seconds)
                    continue
                return None, (
                    f"HTTP {response.status_code} for merchant {merchant_id} | "
                    f"Response: {response.text[:500]}"
                )

            return None, (
                f"HTTP {response.status_code} for merchant {merchant_id} | "
                f"Response: {response.text[:500]}"
            )

        except requests.exceptions.Timeout:
            wait_seconds = min(2 ** (attempt - 1), 8)
            print(
                f"Timeout for merchant {merchant_id} "
                f"(attempt {attempt}/{MAX_RETRIES}). Waiting {wait_seconds}s..."
            )
            if attempt < MAX_RETRIES:
                time.sleep(wait_seconds)
                continue
            return None, f"Request timed out for merchant {merchant_id} after {MAX_RETRIES} attempts"
        except requests.exceptions.RequestException as e:
            wait_seconds = min(2 ** (attempt - 1), 8)
            print(
                f"Request error for merchant {merchant_id} "
                f"(attempt {attempt}/{MAX_RETRIES}): {e}"
            )
            if attempt < MAX_RETRIES:
                time.sleep(wait_seconds)
                continue
            return None, f"Request failed for merchant {merchant_id}: {e}"

    return None, f"Request failed for merchant {merchant_id} after retries"


def fetch_merchant_data_in_range(merchant_id, range_start, range_end):
    """
    Fetch all pages for one merchant within a specific date window.
    """
    page = 1
    per_page = 100
    all_data = []

    while True:
        print(f"Merchant {merchant_id} | Range {range_start} to {range_end} | Page {page}")

        params = {
            "merchant_id": merchant_id,
            "currency": currency,
            "created_after": range_start,
            "created_before": range_end,
            "page": page,
            "per_page": per_page,
            "order_by": "id",
            "sort": "asc",
        }

        result, error = fetch_page_with_retry(params, merchant_id)
        if error:
            return None, error

        payments = extract_payments(result)
        if not payments:
            break

        for payment in payments:
            if isinstance(payment, dict):
                payment["merchantId"] = merchant_id

        all_data.extend(payments)
        print(f"Fetched {len(payments)} records")

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


# ---------------- FETCH FUNCTION ---------------- #
def fetch_merchant_data(merchant_id):
    # First try the full date range.
    data, error = fetch_merchant_data_in_range(merchant_id, start_date, end_date)
    if error is None:
        return data

    # If the broad range is too slow for this merchant, fall back to day-by-day requests.
    print(
        f"Broad range failed for merchant {merchant_id}: {error}\n"
        f"Falling back to daily chunks for {merchant_id}."
    )

    fallback_data = []
    for range_start, range_end in iter_date_windows(start_date, end_date):
        daily_data, daily_error = fetch_merchant_data_in_range(merchant_id, range_start, range_end)
        if daily_error:
            print(f"Daily fetch failed for merchant {merchant_id} on {range_start}: {daily_error}")
            continue
        fallback_data.extend(daily_data)

    return fallback_data


# ---------------- MAIN ---------------- #
def fetch_all_merchants():
    final_data = []
    merchant_counts = {}

    for merchant_id in merchant_ids:
        print(f"\nProcessing merchant: {merchant_id}")

        data = fetch_merchant_data(merchant_id)

        merchant_counts[merchant_id] = len(data)
        final_data.extend(data)

        print(f"Total for merchant {merchant_id}: {len(data)}")

    return final_data, merchant_counts


# ---------------- RUN ---------------- #
if __name__ == "__main__":
    if TOKEN == "PASTE_YOUR_TOKEN_HERE":
        raise SystemExit(
            "Set WEPOUT_TOKEN in your environment or paste the token into TOKEN before running."
        )

    data, merchant_counts = fetch_all_merchants()

    print("\nSummary\n")

    total_records = len(data)

    for mid in merchant_ids:
        print(f"Merchant {mid}: {merchant_counts.get(mid, 0)}")

    print(f"\nTotal Records (Date Filtered): {total_records}")

    # Save output
    with open("payments_full_data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print("\nData saved to payments_full_data.json")

    save_csv(data, "payments_full_data.csv")
    print("Data saved to payments_full_data.csv")

    # Show sample
    print("\nSample Records:\n")
    for record in data[:5]:
        print(json.dumps(record, indent=2, ensure_ascii=False))
