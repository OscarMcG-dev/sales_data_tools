"""Transcripts tab: transcribe JustCall recordings via Mistral Voxtral, sync to Attio."""
import csv
import io
import logging
from datetime import date, datetime, time, timezone

import streamlit as st

from lib.config import Settings
from lib.db import LeadDB
from lib.transcript_processor import (
    COL_RECORDING,
    COL_TRANSCRIPTION,
    fetch_attio_call_records,
    filter_rows_for_transcription,
    load_csv_rows,
    load_url_list,
    SKIP_OUTCOMES_DEFAULT,
    TranscriptProcessor,
)

# Session state keys for persisting loaded data across reruns
KEY_ROWS = "transcripts_rows"
KEY_FIELDNAMES = "transcripts_fieldnames"
KEY_SOURCE = "transcripts_source"


def _run_pipeline_with_log_capture(proc: TranscriptProcessor, rows: list, fieldnames: list, **kwargs) -> tuple[dict, str]:
    """Run run_transcription_pipeline and capture log output. Returns (stats, log_text)."""
    log_buffer = io.StringIO()
    handler = logging.StreamHandler(log_buffer)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    log = logging.getLogger("lib.transcript_processor")
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)
    try:
        stats = proc.run_transcription_pipeline(rows, fieldnames, **kwargs)
        return stats, log_buffer.getvalue()
    finally:
        log.removeHandler(handler)


def render(db: LeadDB, settings: Settings) -> None:
    st.header("Transcripts")
    if not settings.mistral_api_key:
        st.warning("Set MISTRAL_API_KEY to transcribe recordings.")
    if not settings.attio_api_key:
        st.warning("Set ATTIO_API_KEY to sync transcripts to Attio or link calls to People.")

    st.markdown(
        "Transcribe JustCall call recordings (Mistral Voxtral Mini, with speaker labels), "
        "sync to Attio **justcall_call**, and link calls to People. Choose your input below."
    )

    # -------------------------------------------------------------------------
    # Step 1: Choose source & load data
    # -------------------------------------------------------------------------
    st.subheader("Step 1: Choose source & load data")
    source = st.radio(
        "How do you want to provide calls?",
        ["CSV upload", "Fetch from Attio", "Paste URLs"],
        horizontal=True,
        format_func=lambda x: {
            "CSV upload": "📄 Upload a JustCall/Attio CSV export",
            "Fetch from Attio": "🔗 Fetch filtered calls directly from Attio",
            "Paste URLs": "🔗 Paste recording URLs",
        }[x],
    )

    # Persist source so we can clear rows when switching
    if KEY_SOURCE not in st.session_state:
        st.session_state[KEY_SOURCE] = source
    if st.session_state[KEY_SOURCE] != source:
        st.session_state[KEY_SOURCE] = source
        for key in (KEY_ROWS, KEY_FIELDNAMES):
            if key in st.session_state:
                del st.session_state[key]

    rows = []
    fieldnames = []

    if source == "CSV upload":
        uploaded = st.file_uploader(
            "Upload a CSV exported from Attio (JustCall Calls view)",
            type=["csv"],
            help="Export from Attio: JustCall Calls object → Export CSV. Must include columns: Call Recording, Call Transcription, Record ID, etc.",
        )
        if uploaded:
            rows, fieldnames = load_csv_rows(csv_content=uploaded.read())
            st.session_state[KEY_ROWS] = rows
            st.session_state[KEY_FIELDNAMES] = fieldnames
            st.success(f"Loaded **{len(rows)}** rows. You can now go to Step 2.")
        elif KEY_ROWS in st.session_state:
            rows = st.session_state[KEY_ROWS]
            fieldnames = st.session_state.get(KEY_FIELDNAMES, [])

    elif source == "Fetch from Attio":
        if not settings.attio_api_key:
            st.error("ATTIO_API_KEY is required to fetch calls from Attio.")
        else:
            st.caption("Fetch JustCall call records from Attio with filters. After fetching, use Step 2 to choose which to transcribe.")
            col1, col2 = st.columns(2)
            with col1:
                created_after_date = st.date_input(
                    "Created on or after",
                    value=date(date.today().year, 1, 1),
                    help="Only include calls created on or after this date (start of day UTC).",
                )
                skip_outcomes_attio = st.text_input(
                    "Skip outcomes (comma-separated)",
                    value=",".join(SKIP_OUTCOMES_DEFAULT),
                    help="Exclude calls whose outcome contains any of these (e.g. hit voicemail).",
                )
            with col2:
                created_before_date = st.date_input(
                    "Created before",
                    value=date.today(),
                    help="Only include calls created before this date (end of day UTC).",
                )
                attio_limit = st.number_input(
                    "Max records to fetch",
                    min_value=1,
                    max_value=500,
                    value=100,
                    help="Attio returns newest first; this caps how many to load.",
                )
            if st.button("Fetch calls from Attio"):
                with st.spinner("Fetching…"):
                    skip_list = [s.strip() for s in (skip_outcomes_attio or "").split(",") if s.strip()]
                    created_after_iso = datetime.combine(created_after_date, time(0, 0, 0), tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    created_before_iso = datetime.combine(created_before_date, time(23, 59, 59), tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    rows = fetch_attio_call_records(
                        settings.attio_api_key,
                        skip_outcomes=skip_list or None,
                        created_after=created_after_iso,
                        created_before=created_before_iso,
                        limit=attio_limit,
                    )
                    fieldnames = [
                        "Record ID",
                        "Call Recording",
                        "Call Transcription",
                        "Call Duration (Seconds)",
                        "Call ID",
                        "Contact Name",
                        "Call Outcome",
                        "Appointment Setter",
                        "Contact Name > Record ID",
                    ]
                    st.session_state[KEY_ROWS] = rows
                    st.session_state[KEY_FIELDNAMES] = fieldnames
                st.success(f"Fetched **{len(rows)}** call records. You can now go to Step 2.")
            if KEY_ROWS in st.session_state and st.session_state[KEY_ROWS]:
                rows = st.session_state[KEY_ROWS]
                fieldnames = st.session_state.get(KEY_FIELDNAMES, [])
                with st.expander("Preview fetched calls"):
                    st.dataframe(rows[:20], width="stretch")

    else:
        url_input = st.text_area(
            "Paste recording URLs",
            placeholder="One URL per line, or comma-separated. Optional: add a second column with Attio record_id to sync back.",
            height=120,
            help="Paste one or more recording URLs. Optionally add a comma and the Attio justcall_call record_id to push the transcript back.",
        )
        if url_input.strip():
            rows = load_url_list(url_input.strip())
            fieldnames = [
                "Record ID",
                "Call Recording",
                "Call Transcription",
                "Call Duration (Seconds)",
                "Call ID",
                "Contact Name",
                "Call Outcome",
                "Appointment Setter",
                "Contact Name > Record ID",
            ]
            st.session_state[KEY_ROWS] = rows
            st.session_state[KEY_FIELDNAMES] = fieldnames
            st.success(f"Parsed **{len(rows)}** URL(s). You can now go to Step 2.")
        else:
            # Clear stale URL-based data when text area is empty
            for key in (KEY_ROWS, KEY_FIELDNAMES):
                if key in st.session_state:
                    del st.session_state[key]

    # If we have rows from a previous action but not in current scope (e.g. after Attio fetch)
    if not rows and KEY_ROWS in st.session_state:
        rows = st.session_state[KEY_ROWS]
        fieldnames = st.session_state.get(KEY_FIELDNAMES, [])

    has_recording_column = COL_RECORDING in (fieldnames or []) or any((r.get(COL_RECORDING) or "").strip() for r in (rows or []))
    if not rows or not has_recording_column:
        st.info("Load or fetch some calls above (with recording URLs), then Step 2 will appear.")
        return

    # -------------------------------------------------------------------------
    # Step 2: Configure & run
    # -------------------------------------------------------------------------
    st.subheader("Step 2: Configure & run")
    st.caption("Choose which calls to transcribe and what to do with the results.")

    with st.expander("Transcription options", expanded=True):
        st.markdown("**Which calls to transcribe**")
        opt_col1, opt_col2 = st.columns(2)
        with opt_col1:
            min_duration = st.number_input(
                "Min call duration (seconds)",
                min_value=0,
                value=15,
                help="Skip calls shorter than this (e.g. 15 to avoid very short rings).",
            )
            skip_outcomes = st.text_input(
                "Skip outcomes (comma-separated)",
                value=",".join(SKIP_OUTCOMES_DEFAULT),
                key="skip_outcomes",
                help="Exclude calls whose outcome contains any of these (e.g. hit voicemail).",
            )
            max_calls = st.number_input(
                "Max calls to transcribe (0 = all)",
                min_value=0,
                value=0,
                help="Cap how many to process (e.g. 5 for a quick test).",
            )
        with opt_col2:
            skip_existing = st.checkbox(
                "Only transcribe calls without existing transcript",
                value=True,
                help="Skip rows that already have text in Call Transcription (recommended for Attio fetch).",
            )
            dry_run = st.checkbox(
                "Dry run (no API calls)",
                value=False,
                help="Simulate run: no Mistral or Attio calls; placeholder transcripts only.",
            )

        st.markdown("**After transcription**")
        after_col1, after_col2 = st.columns(2)
        with after_col1:
            sync_attio = st.checkbox(
                "Sync transcripts to Attio",
                value=bool(settings.attio_api_key),
                help="PATCH each call's call_transcription on the justcall_call record.",
            )
            link_all_calls = st.checkbox(
                "Link calls to People in Attio",
                value=True,
                help="Set contact_name and calls relationship so calls appear on the person.",
            )

    # Eligible count
    skip_list = [s.strip() for s in (skip_outcomes or "").split(",") if s.strip()]
    to_transcribe, filter_stats = filter_rows_for_transcription(
        rows,
        min_duration=min_duration,
        skip_outcomes=skip_list or None,
        skip_existing_transcript=skip_existing,
    )
    eligible = len(to_transcribe)
    if max_calls and max_calls > 0:
        will_process = min(eligible, max_calls)
    else:
        will_process = eligible
    st.caption(
        f"**{will_process}** call(s) will be transcribed "
        f"(from {len(rows)} loaded, {eligible} pass filters"
        + (f", max {max_calls}" if max_calls and max_calls > 0 else "")
        + ")."
    )
    with st.expander("Filter breakdown"):
        st.write(
            f"No recording URL: {filter_stats.get('no_url', 0)} · "
            f"Skipped outcome (e.g. voicemail): {filter_stats.get('voicemail', 0)} · "
            f"Already have transcript: {filter_stats.get('has_transcript', 0)} · "
            f"Below min duration: {filter_stats.get('below_duration', 0)}"
        )

    run_transcription = st.button("Transcribe and sync to Attio", type="primary")

    if run_transcription and rows and fieldnames:
        if not settings.mistral_api_key and not dry_run:
            st.error("MISTRAL_API_KEY is required to transcribe. Set it in .env or use Dry run.")
        else:
            proc = TranscriptProcessor(settings.mistral_api_key, settings.attio_api_key)
            max_calls_val = None if (max_calls is None or max_calls <= 0) else max_calls
            with st.spinner("Transcribing and syncing…"):
                stats, log_text = _run_pipeline_with_log_capture(
                    proc,
                    rows,
                    fieldnames,
                    min_duration=min_duration,
                    skip_outcomes=skip_list or None,
                    skip_existing_transcript=skip_existing,
                    max_calls=max_calls_val,
                    dry_run=dry_run,
                    sync_attio=sync_attio,
                    link_all_calls_to_people=link_all_calls,
                )
            succeeded = stats.get("succeeded", 0)
            failed = stats.get("failed", 0)
            attio_updated = stats.get("attio_updated", 0)
            links_ok = stats.get("links_ok", 0)
            st.success(
                f"Done. Transcribed: **{succeeded}** succeeded, **{failed}** failed. "
                + (f"Synced to Attio: **{attio_updated}** transcripts, **{links_ok}** call–people links." if sync_attio else "")
            )
            with st.expander("Details (counts and filter stats)"):
                st.json({
                    "filter": stats.get("filter", {}),
                    "succeeded": succeeded,
                    "failed": failed,
                    "skipped": stats.get("skipped", 0),
                    "attio_updated": attio_updated,
                    "attio_failed": stats.get("attio_failed", 0),
                    "links_ok": links_ok,
                    "links_fail": stats.get("links_fail", 0),
                })
            dl_col1, dl_col2 = st.columns(2)
            with dl_col1:
                buf = io.BytesIO()
                out_rows = [{k: r.get(k) for k in fieldnames} for r in rows]
                text_buf = io.StringIO()
                writer = csv.DictWriter(text_buf, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
                writer.writeheader()
                writer.writerows(out_rows)
                buf.write(text_buf.getvalue().encode("utf-8"))
                buf.seek(0)
                st.download_button("Download result CSV", data=buf, file_name="transcripts_export.csv", mime="text/csv")
            with dl_col2:
                log_bytes = io.BytesIO(log_text.encode("utf-8"))
                st.download_button("Download run log", data=log_bytes, file_name="transcripts_run.log", mime="text/plain")
