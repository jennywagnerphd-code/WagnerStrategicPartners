import csv
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import requests


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass
class Config:
    """
    Configuration for the opportunity intelligence pipeline.

    Set these as environment variables before running:
      - OPPORTUNITY_API_URL
      - OPPORTUNITY_API_KEY (optional)
      - OUTPUT_CSV (optional)
      - KEYWORDS (optional, comma-separated)
      - TIMEOUT_SECONDS (optional)
    """

    api_url: str = field(default_factory=lambda: os.getenv("OPPORTUNITY_API_URL", "").strip())
    api_key: str = field(default_factory=lambda: os.getenv("OPPORTUNITY_API_KEY", "").strip())
    output_csv: str = field(default_factory=lambda: os.getenv("OUTPUT_CSV", "health_opportunity_digest.csv"))
    timeout_seconds: int = field(default_factory=lambda: int(os.getenv("TIMEOUT_SECONDS", "30")))
    keywords: List[str] = field(
        default_factory=lambda: [
            keyword.strip().lower()
            for keyword in os.getenv(
                "KEYWORDS",
                "health,healthcare,medical,clinical,hospital,public health,biomedical,fda,nih,cdc,hhs,dha"
            ).split(",")
            if keyword.strip()
        ]
    )


def build_headers(config: Config) -> Dict[str, str]:
    """
    Build request headers for the API call.
    Adjust this if your API uses a different auth scheme.
    """
    headers = {
        "Accept": "application/json",
        "User-Agent": "health-opportunity-intelligence-pipeline/1.0",
    }

    if config.api_key:
        headers["Authorization"] = f"Bearer {config.api_key}"

    return headers


def fetch_opportunities(config: Config) -> List[Dict[str, Any]]:
    """
    Fetch raw opportunities from the configured API endpoint.

    Assumes the API returns either:
      - a list of opportunity records, or
      - a dict containing a list under a common key like 'results' or 'opportunities'
    """
    if not config.api_url:
        raise ValueError("OPPORTUNITY_API_URL is not set.")

    logger.info("Requesting opportunity data from API...")
    response = requests.get(
        config.api_url,
        headers=build_headers(config),
        timeout=config.timeout_seconds,
    )
    response.raise_for_status()

    payload = response.json()

    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, dict):
        records = (
            payload.get("results")
            or payload.get("opportunities")
            or payload.get("data")
            or []
        )
    else:
        raise ValueError("Unexpected API response format.")

    if not isinstance(records, list):
        raise ValueError("Expected a list of opportunity records from the API.")

    logger.info("Fetched %s raw records.", len(records))
    return records


def safe_get(record: Dict[str, Any], *keys: str) -> str:
    """
    Return the first non-empty value among several possible keys.
    Converts values to strings and strips whitespace.
    """
    for key in keys:
        value = record.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def normalize_record(record: Dict[str, Any]) -> Dict[str, str]:
    """
    Normalize source records into a consistent schema.

    Update the fallback key list to match your source API.
    """
    title = safe_get(record, "title", "name", "opportunity_title")
    summary = safe_get(record, "summary", "description", "synopsis")
    agency = safe_get(record, "agency", "department", "organization")
    posted_date = safe_get(record, "posted_date", "publish_date", "date", "created_at")
    due_date = safe_get(record, "due_date", "response_deadline", "deadline", "close_date")
    link = safe_get(record, "url", "link", "notice_url", "opportunity_url")
    opportunity_id = safe_get(record, "id", "notice_id", "opportunity_id", "solicitation_number")

    searchable_text = " ".join(
        [
            title.lower(),
            summary.lower(),
            agency.lower(),
        ]
    )

    return {
        "opportunity_id": opportunity_id,
        "title": title,
        "agency": agency,
        "posted_date": posted_date,
        "due_date": due_date,
        "summary": summary,
        "link": link,
        "searchable_text": searchable_text,
    }


def keyword_matches(text: str, keywords: Iterable[str]) -> List[str]:
    """
    Return the list of matched keywords in a block of text.
    """
    matches = [keyword for keyword in keywords if keyword in text]
    return sorted(set(matches))


def filter_relevant_opportunities(
    records: List[Dict[str, Any]],
    keywords: List[str],
) -> List[Dict[str, str]]:
    """
    Filter raw records to those relevant to health-related opportunities.
    """
    filtered: List[Dict[str, str]] = []

    for raw_record in records:
        normalized = normalize_record(raw_record)
        matches = keyword_matches(normalized["searchable_text"], keywords)

        if matches:
            normalized["matched_keywords"] = ", ".join(matches)
            filtered.append(normalized)

    logger.info("Filtered down to %s relevant records.", len(filtered))
    return filtered


def score_record(record: Dict[str, str]) -> int:
    """
    Basic relevance scoring based on matched keyword count and presence of useful fields.
    This keeps the project portfolio-friendly and AI-ready without overstating complexity.
    """
    score = 0

    matched_keywords = record.get("matched_keywords", "")
    if matched_keywords:
        score += len([k for k in matched_keywords.split(",") if k.strip()]) * 10

    if record.get("agency"):
        score += 5
    if record.get("due_date"):
        score += 5
    if record.get("summary"):
        score += 5
    if record.get("link"):
        score += 5

    return score


def rank_opportunities(records: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Add a simple relevance score and sort descending.
    """
    for record in records:
        record["relevance_score"] = str(score_record(record))

    ranked = sorted(
        records,
        key=lambda r: int(r["relevance_score"]),
        reverse=True,
    )

    return ranked


def export_to_csv(records: List[Dict[str, str]], output_path: str) -> None:
    """
    Export filtered records to CSV.
    """
    fieldnames = [
        "opportunity_id",
        "title",
        "agency",
        "posted_date",
        "due_date",
        "matched_keywords",
        "relevance_score",
        "summary",
        "link",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        for record in records:
            writer.writerow({field: record.get(field, "") for field in fieldnames})

    logger.info("Wrote digest to %s", output_path)


def print_console_summary(records: List[Dict[str, str]], max_items: int = 10) -> None:
    """
    Print a compact summary of top opportunities to the console.
    """
    logger.info("Top %s opportunities:", min(len(records), max_items))

    for index, record in enumerate(records[:max_items], start=1):
        logger.info(
            "%s. %s | %s | Due: %s | Score: %s",
            index,
            record.get("title", "Untitled"),
            record.get("agency", "Unknown agency"),
            record.get("due_date", "N/A"),
            record.get("relevance_score", "0"),
        )


def main() -> None:
    """
    Main workflow:
      1. Load configuration
      2. Fetch opportunities from API
      3. Filter for health relevance
      4. Rank results
      5. Export CSV
      6. Print top summary
    """
    try:
        config = Config()

        logger.info("Starting health opportunity intelligence pipeline...")
        raw_records = fetch_opportunities(config)
        filtered_records = filter_relevant_opportunities(raw_records, config.keywords)
        ranked_records = rank_opportunities(filtered_records)
        export_to_csv(ranked_records, config.output_csv)
        print_console_summary(ranked_records)

        logger.info("Pipeline completed successfully.")

    except requests.HTTPError as exc:
        logger.exception("HTTP error while calling the API: %s", exc)
        raise
    except requests.RequestException as exc:
        logger.exception("Network or request error: %s", exc)
        raise
    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        raise


if __name__ == "__main__":
    main()
