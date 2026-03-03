#!/usr/bin/env python3
"""
CLI: Transcribe JustCall call recordings via Mistral Voxtral Mini, sync to Attio.

Uses lib.transcript_processor. Supports CSV upload, Attio API fetch, or URL list.
Usage:
  python transcribe_calls.py --csv "JustCall Calls - export.csv"
  python transcribe_calls.py --source attio --no-transcript-only --max-calls 5
  python transcribe_calls.py --source urls --urls "https://example.com/rec.mp3" --min-duration 0 --dry-run --no-attio
"""
import argparse
import csv
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

from lib.transcript_processor import (
    COL_CONTACT,
    COL_CALL_ID,
    COL_PEOPLE_RECORD_ID,
    COL_RECORD_ID,
    COL_TRANSCRIPTION,
    fetch_attio_call_records,
    link_attio_call_to_people,
    load_csv_rows,
    load_url_list,
    update_attio_call_transcript,
    SKIP_OUTCOMES_DEFAULT,
    TranscriptProcessor,
)


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("transcribe_calls")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    fh = logging.FileHandler(Path(__file__).resolve().parent / "logs" / "transcribe_calls.log", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(levelname)s | %(message)s"))
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def main() -> None:
    parser = argparse.ArgumentParser(description="Transcribe JustCall recordings via Mistral Voxtral, sync to Attio")
    parser.add_argument("--source", choices=("csv", "attio", "urls"), default="csv")
    parser.add_argument("--csv", default="Calls_to_transcribe.csv")
    parser.add_argument("--urls", default=None)
    parser.add_argument("--min-duration", type=int, default=15)
    parser.add_argument("--output", default=None)
    parser.add_argument("--max-calls", type=int, default=None)
    parser.add_argument("--skip-existing", action="store_true", help="Skip rows that already have a transcript")
    parser.add_argument("--no-attio", action="store_true")
    parser.add_argument("--attio-only", action="store_true", help="Only push existing CSV transcripts to Attio")
    parser.add_argument("--link-only", action="store_true", help="Only link call records to People")
    parser.add_argument("--no-transcript-only", action="store_true", help="Only fetch Attio records without transcript")
    parser.add_argument("--skip-outcomes", type=str, default=None)
    parser.add_argument("--created-after", type=str, default=None)
    parser.add_argument("--created-before", type=str, default=None)
    parser.add_argument("--setters", type=str, default=None)
    parser.add_argument("--attio-query-limit", type=int, default=500)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logger = setup_logging()
    Path(__file__).resolve().parent.joinpath("logs").mkdir(exist_ok=True)

    attio_token = os.environ.get("ATTIO_API_TOKEN") or os.environ.get("ATTIO_API_KEY") or ""
    attio_enabled = bool(attio_token) and not args.no_attio

    # Resolve input
    fieldnames = []
    rows = []
    output_path = Path(args.output) if args.output else None

    if args.source == "attio":
        if not attio_token:
            logger.error("--source attio requires ATTIO_API_TOKEN or ATTIO_API_KEY")
            sys.exit(1)
        skip_list = [s.strip() for s in (args.skip_outcomes or "").split(",") if s.strip()] or SKIP_OUTCOMES_DEFAULT
        setters = [s.strip() for s in (args.setters or "").split(",") if s.strip()] or None
        rows = fetch_attio_call_records(
            attio_token,
            no_transcript_only=args.no_transcript_only,
            skip_outcomes=skip_list,
            created_after=args.created_after,
            created_before=args.created_before,
            setters=setters,
            limit=args.attio_query_limit,
        )
        fieldnames = [
            COL_RECORD_ID, "Call Recording", COL_TRANSCRIPTION, "Call Duration (Seconds)", COL_CALL_ID,
            COL_CONTACT, "Call Outcome", "Appointment Setter", COL_PEOPLE_RECORD_ID,
        ]
        logger.info("Fetched %s records from Attio", len(rows))

    elif args.source == "urls":
        if not args.urls:
            logger.error("--source urls requires --urls")
            sys.exit(1)
        rows = load_url_list(args.urls)
        fieldnames = [
            COL_RECORD_ID, "Call Recording", COL_TRANSCRIPTION, "Call Duration (Seconds)", COL_CALL_ID,
            COL_CONTACT, "Call Outcome", "Appointment Setter", COL_PEOPLE_RECORD_ID,
        ]
        logger.info("Loaded %s URLs", len(rows))

    else:
        csv_path = Path(args.csv)
        for base in [Path(__file__).resolve().parent, Path(__file__).resolve().parent / "logic_to_port"]:
            cand = base / args.csv
            if cand.exists():
                csv_path = cand
                break
        if not csv_path.exists():
            logger.error("CSV not found: %s", args.csv)
            sys.exit(1)
        rows, fieldnames = load_csv_rows(csv_path)
        logger.info("Read %s rows from %s", len(rows), csv_path)

    # Attio-only mode
    if args.attio_only:
        if not attio_token:
            logger.error("--attio-only requires ATTIO_API_TOKEN or ATTIO_API_KEY")
            sys.exit(1)
        if args.source != "csv":
            logger.error("--attio-only only supports --source csv")
            sys.exit(1)
        to_push = [
            r for r in rows
            if (r.get(COL_RECORD_ID) or "").strip()
            and (r.get(COL_TRANSCRIPTION) or "").strip()
            and not (r.get(COL_TRANSCRIPTION) or "").strip().startswith("[TRANSCRIPTION_ERROR")
        ]
        if args.max_calls:
            to_push = to_push[: args.max_calls]
        for idx, row in enumerate(to_push, 1):
            rid = row[COL_RECORD_ID].strip()
            transcript = row[COL_TRANSCRIPTION].strip()
            if update_attio_call_transcript(rid, transcript, attio_token):
                logger.info("[%s/%s] Pushed %s chars to %s", idx, len(to_push), len(transcript), rid[:12])
            else:
                logger.warning("[%s/%s] FAILED %s", idx, len(to_push), rid[:12])
        logger.info("Attio-only done. Pushed %s records.", len(to_push))
        return

    # Link-only mode
    if args.link_only:
        if not attio_token:
            logger.error("--link-only requires ATTIO_API_TOKEN or ATTIO_API_KEY")
            sys.exit(1)
        to_link = [
            (r.get(COL_RECORD_ID, "").strip(), r.get(COL_PEOPLE_RECORD_ID, "").strip())
            for r in rows
            if (r.get(COL_RECORD_ID) or "").strip()
            and (r.get(COL_PEOPLE_RECORD_ID) or "").strip()
            and not r.get("_already_linked", False)
        ]
        ok = sum(1 for rid, pid in to_link if link_attio_call_to_people(rid, pid, attio_token))
        logger.info("Link-only done. Linked %s / %s", ok, len(to_link))
        return

    # Normal transcription
    if args.dry_run:
        logger.info("DRY RUN: no Mistral/Attio calls")
    mistral_key = os.environ.get("MISTRAL_API_KEY", "")
    if not args.dry_run and not mistral_key:
        logger.error("MISTRAL_API_KEY required for transcription (or use --dry-run)")
        sys.exit(1)

    proc = TranscriptProcessor(mistral_api_key=mistral_key, attio_api_key=attio_token)
    skip_list = [s.strip() for s in (args.skip_outcomes or "").split(",") if s.strip()] or SKIP_OUTCOMES_DEFAULT
    stats = proc.run_transcription_pipeline(
        rows,
        fieldnames,
        min_duration=args.min_duration,
        skip_outcomes=skip_list,
        skip_existing_transcript=args.skip_existing or (args.source == "attio" and args.no_transcript_only),
        max_calls=args.max_calls,
        dry_run=args.dry_run,
        sync_attio=attio_enabled,
        link_all_calls_to_people=True,
    )
    logger.info(
        "Done. Succeeded=%s failed=%s skipped=%s attio_updated=%s links_ok=%s",
        stats["succeeded"], stats["failed"], stats["skipped"],
        stats["attio_updated"], stats["links_ok"],
    )

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows([{k: r.get(k) for k in fieldnames} for r in rows])
        logger.info("Wrote %s", output_path)


if __name__ == "__main__":
    main()
