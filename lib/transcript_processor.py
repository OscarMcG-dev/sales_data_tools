"""
Process call transcripts: transcribe JustCall recordings via Mistral Voxtral Mini,
format with speaker labels, sync to Attio justcall_call, and link calls to People.

Supports: CSV (JustCall export), Attio API fetch, or URL list.
Optional: summarise existing transcript text via Mistral chat (call_summary, ai_insights).
"""
import csv
import io
import logging
import time
from pathlib import Path
from typing import Any, Optional

import httpx
from mistralai import Mistral

from lib.attio_client import ATTIO_API_BASE

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config (from logic_to_port / REFERENCE.md)
# ---------------------------------------------------------------------------

TRANSCRIPTION_MODEL = "voxtral-mini-latest"
ATTIO_OBJECT_JUSTCALL_CALL = "justcall_call"

CONTEXT_BIAS = [
    "Law_Cyborg",
    "CAANZ",
    "ATO",
    "CCH",
    "Thomson_Reuters",
    "LexisNexis",
    "MYOB",
    "Xero",
    "BGL",
    "EY",
    "bookkeeper",
    "accountant",
    "practitioner",
]

SKIP_OUTCOMES_DEFAULT = ["hit voicemail"]

# CSV column names (Attio JustCall export)
COL_RECORDING = "Call Recording"
COL_TRANSCRIPTION = "Call Transcription"
COL_DURATION = "Call Duration (Seconds)"
COL_CALL_ID = "Call ID"
COL_CONTACT = "Contact Name"
COL_OUTCOME = "Call Outcome"
COL_SETTER = "Appointment Setter"
COL_RECORD_ID = "Record ID"
COL_PEOPLE_RECORD_ID = "Contact Name > Record ID"

# Attio attribute IDs (for reading from API when slug differs)
ATTIO_ATTR_IDS = {
    "call_duration_seconds": "627cb43a-5b93-4f6b-9ef6-3ceeac5f64ef",
    "call_outcome": "793a407b-d0dd-468b-824f-ae6ca3738eb1",
}

ATTIO_QUERY_JUSTCALL_URL = f"{ATTIO_API_BASE}/objects/{ATTIO_OBJECT_JUSTCALL_CALL}/records/query"


def _attio_values_get(values: dict, slug: str) -> Any:
    """Get attribute value from Attio record values (keyed by slug or attribute ID)."""
    if not values:
        return None
    out = values.get(slug)
    if out is not None:
        return out
    attr_id = ATTIO_ATTR_IDS.get(slug)
    if attr_id:
        return values.get(attr_id)
    return None


def _attio_scalar(val: Any) -> Any:
    """Get the current scalar value from an Attio attribute (list of value objects or single)."""
    if val is None:
        return None
    if isinstance(val, list) and len(val) > 0:
        val = val[0]
    if isinstance(val, dict):
        return val.get("value") if "value" in val else val.get("title")
    return val


def _attio_value_text(val: Any) -> str:
    """Extract text from an Attio attribute value."""
    scalar = _attio_scalar(val)
    if scalar is None:
        return ""
    if isinstance(scalar, str):
        return scalar.strip()
    return str(scalar).strip()


# ---------------------------------------------------------------------------
# Diarization / speaker labelling (from REFERENCE § Speaker labelling)
# ---------------------------------------------------------------------------


def format_diarized_transcript(segments: list, rep_name: Optional[str] = None) -> str:
    """Format diarized segments into a readable transcript with speaker labels.

    Rep = first speaker whose first 3 text blocks contain the first name from
    Appointment Setter (case-insensitive). Fallback: if 2+ speakers and no match,
    second speaker = Rep, first = Prospect. Labels: Rep → first name or "Rep";
    other main → "Prospect"; extra → "Speaker 3", "Speaker 4".
    """
    if not segments:
        return ""

    merged = []
    for seg in segments:
        speaker = getattr(seg, "speaker_id", None) or "unknown"
        text = (getattr(seg, "text", None) or "").strip()
        if not text:
            continue
        if merged and merged[-1]["speaker"] == speaker:
            merged[-1]["text"] += " " + text
        else:
            merged.append({"speaker": speaker, "text": text})

    rep_first_name = (rep_name or "").split()[0] or None
    seen_ids = []
    for block in merged:
        if block["speaker"] not in seen_ids:
            seen_ids.append(block["speaker"])

    rep_speaker_id = None
    if rep_first_name:
        for sid in seen_ids:
            speaker_texts = [b["text"] for b in merged if b["speaker"] == sid][:3]
            combined = " ".join(speaker_texts).lower()
            if rep_first_name.lower() in combined:
                rep_speaker_id = sid
                break
    if rep_speaker_id is None and len(seen_ids) >= 2:
        rep_speaker_id = seen_ids[1]

    speaker_map = {}
    for sid in seen_ids:
        if sid == rep_speaker_id and rep_first_name:
            speaker_map[sid] = rep_first_name
        elif sid == rep_speaker_id:
            speaker_map[sid] = "Rep"
        else:
            if "Prospect" not in (speaker_map.values() or []):
                speaker_map[sid] = "Prospect"
            else:
                n = sum(1 for v in speaker_map.values() if str(v).startswith("Speaker"))
                speaker_map[sid] = f"Speaker {n + 3}"

    lines = [f"{speaker_map.get(block['speaker'], block['speaker'])}: {block['text']}" for block in merged]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Transcription API (Mistral Voxtral)
# ---------------------------------------------------------------------------


def transcribe_url(client: Mistral, audio_url: str, rep_name: Optional[str] = None) -> str:
    """Transcribe an audio URL via Mistral Voxtral Mini; diarization and context_bias on."""
    try:
        response = client.audio.transcriptions.complete(
            model=TRANSCRIPTION_MODEL,
            file_url=audio_url,
            diarize=True,
            timestamp_granularities=["segment"],
            context_bias=CONTEXT_BIAS,
        )
        if response.segments:
            return format_diarized_transcript(response.segments, rep_name=rep_name)
        return (response.text or "").strip() if response else ""
    except Exception as e:
        logger.warning("Transcription error: %s", e)
        return f"[TRANSCRIPTION_ERROR: {e}]"


# ---------------------------------------------------------------------------
# Attio — update justcall_call record and link to People
# ---------------------------------------------------------------------------


def update_attio_call_transcript(record_id: str, transcript: str, attio_token: str) -> bool:
    """PATCH justcall_call record: set call_transcription. Returns True on success."""
    if not record_id or not attio_token:
        return False
    url = f"{ATTIO_API_BASE}/objects/{ATTIO_OBJECT_JUSTCALL_CALL}/records/{record_id}"
    headers = {
        "Authorization": f"Bearer {attio_token}",
        "Content-Type": "application/json",
    }
    # Attio text attributes: use list of value objects to be consistent with other objects
    payload = {
        "data": {
            "values": {
                "call_transcription": [{"value": transcript[:50000]}],
            }
        }
    }
    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.patch(url, json=payload, headers=headers)
        if resp.status_code == 200:
            return True
        logger.warning("Attio PATCH transcript %s: %s %s", resp.status_code, record_id[:12], resp.text[:200])
        return False
    except Exception as e:
        logger.warning("Attio PATCH transcript error: %s", e)
        return False


def link_attio_call_to_people(record_id: str, people_record_id: str, attio_token: str) -> bool:
    """Link justcall_call to people: set contact_name and calls relationship."""
    if not people_record_id or not record_id or not attio_token:
        return False
    url = f"{ATTIO_API_BASE}/objects/{ATTIO_OBJECT_JUSTCALL_CALL}/records/{record_id}"
    headers = {
        "Authorization": f"Bearer {attio_token}",
        "Content-Type": "application/json",
    }
    people_ref = {"target_object": "people", "target_record_id": people_record_id}
    payload = {
        "data": {
            "values": {
                "contact_name": [people_ref],
                "calls": people_ref,
            }
        }
    }
    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.patch(url, json=payload, headers=headers)
        if resp.status_code == 200:
            return True
        logger.warning("Attio link people %s: %s %s", resp.status_code, record_id[:12], resp.text[:200])
        return False
    except Exception as e:
        logger.warning("Attio link people error: %s", e)
        return False


# ---------------------------------------------------------------------------
# Attio — query justcall_call records
# ---------------------------------------------------------------------------


def fetch_attio_call_records(
    attio_token: str,
    *,
    no_transcript_only: bool = False,
    skip_outcomes: Optional[list] = None,
    created_after: Optional[str] = None,
    created_before: Optional[str] = None,
    setters: Optional[list] = None,
    limit: int = 500,
) -> list[dict]:
    """Query Attio justcall_call records; return list of row dicts (same shape as CSV rows)."""
    if skip_outcomes is None:
        skip_outcomes = []
    clauses = []
    for outcome in skip_outcomes:
        clauses.append({"$not": {"call_outcome": {"$contains": outcome}}})
    if created_after:
        clauses.append({"created_at": {"$gte": created_after}})
    if created_before:
        clauses.append({"created_at": {"$lte": created_before}})
    if setters:
        clauses.append({"appointment_setter": {"$in": setters}})

    body = {"limit": min(limit, 100)}
    if clauses:
        body["filter"] = {"$and": clauses} if len(clauses) > 1 else clauses[0]
    body["sorts"] = [{"attribute": "created_at", "direction": "desc"}]

    headers = {
        "Authorization": f"Bearer {attio_token}",
        "Content-Type": "application/json",
    }
    all_rows = []
    offset = 0
    while True:
        payload = {**body, "offset": offset}
        try:
            with httpx.Client(timeout=30.0) as client:
                resp = client.post(ATTIO_QUERY_JUSTCALL_URL, json=payload, headers=headers)
            if resp.status_code != 200:
                logger.warning("Attio query justcall_call %s: %s", resp.status_code, resp.text[:300])
                break
            data = resp.json()
            records = data.get("data") or []
            if not records:
                break
            for rec in records:
                record_id = (rec.get("id") or {}).get("record_id") or ""
                values = rec.get("values") or {}
                dur_scalar = _attio_scalar(_attio_values_get(values, "call_duration_seconds"))
                dur_str = str(int(dur_scalar)) if isinstance(dur_scalar, (int, float)) else "0"
                row = {
                    COL_RECORD_ID: record_id,
                    COL_RECORDING: _attio_value_text(values.get("call_recording")),
                    COL_TRANSCRIPTION: _attio_value_text(values.get("call_transcription")),
                    COL_DURATION: dur_str,
                    COL_CALL_ID: _attio_value_text(values.get("call_id")) or "?",
                    COL_CONTACT: _attio_value_text(values.get("contact_name")),
                    COL_OUTCOME: _attio_value_text(_attio_values_get(values, "call_outcome")),
                    COL_SETTER: _attio_value_text(values.get("appointment_setter")),
                }
                contact_ref = values.get("contact_name")
                if isinstance(contact_ref, list) and len(contact_ref) > 0:
                    contact_ref = contact_ref[0]
                if isinstance(contact_ref, dict) and contact_ref.get("target_record_id"):
                    row[COL_PEOPLE_RECORD_ID] = contact_ref["target_record_id"]
                    row["_already_linked"] = True
                else:
                    row[COL_PEOPLE_RECORD_ID] = ""
                    row["_already_linked"] = False
                all_rows.append(row)
            if len(records) < body["limit"]:
                break
            offset += len(records)
            if len(all_rows) >= limit:
                break
            time.sleep(0.2)
        except Exception as e:
            logger.warning("Attio query justcall_call error: %s", e)
            break
    return all_rows[:limit]


# ---------------------------------------------------------------------------
# Load CSV and URL list
# ---------------------------------------------------------------------------


def load_csv_rows(csv_path: Optional[Path] = None, csv_content: Optional[bytes] = None) -> tuple[list[dict], list[str]]:
    """Load rows from CSV file or bytes. Returns (rows, fieldnames)."""
    if csv_content is not None:
        text = csv_content.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        fieldnames = list(reader.fieldnames or [])
        return list(reader), fieldnames
    if csv_path and csv_path.exists():
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = list(reader.fieldnames or [])
            return list(reader), fieldnames
    return [], []


def load_url_list(urls_arg: str) -> list[dict]:
    """Load list of recording URLs from file path or comma-separated string.
    Returns list of row dicts with COL_RECORDING; optional second column = record_id for Attio sync.
    """
    urls_arg = (urls_arg or "").strip()
    if not urls_arg:
        return []
    rows = []
    path = Path(urls_arg)
    if path.exists() and path.is_file():
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = [p.strip() for p in line.split(",", 1)]
                url = parts[0]
                record_id = parts[1] if len(parts) > 1 else ""
                rows.append({
                    COL_RECORDING: url,
                    COL_TRANSCRIPTION: "",
                    COL_DURATION: "0",
                    COL_CALL_ID: "?",
                    COL_CONTACT: "?",
                    COL_OUTCOME: "",
                    COL_SETTER: "",
                    COL_RECORD_ID: record_id,
                    COL_PEOPLE_RECORD_ID: "",
                })
    else:
        for url in (u.strip() for u in urls_arg.split(",") if u.strip()):
            rows.append({
                COL_RECORDING: url,
                COL_TRANSCRIPTION: "",
                COL_DURATION: "0",
                COL_CALL_ID: "?",
                COL_CONTACT: "?",
                COL_OUTCOME: "",
                COL_SETTER: "",
                COL_RECORD_ID: "",
                COL_PEOPLE_RECORD_ID: "",
            })
    return rows


# ---------------------------------------------------------------------------
# Filter rows for transcription
# ---------------------------------------------------------------------------


def filter_rows_for_transcription(
    rows: list[dict],
    *,
    min_duration: int = 15,
    skip_outcomes: Optional[list] = None,
    skip_existing_transcript: bool = False,
) -> tuple[list[tuple[int, dict]], dict]:
    """Return (to_transcribe, stats). to_transcribe is list of (row_index, row)."""
    if skip_outcomes is None:
        skip_outcomes = SKIP_OUTCOMES_DEFAULT
    to_transcribe = []
    stats = {"no_url": 0, "voicemail": 0, "has_transcript": 0, "below_duration": 0}
    for i, row in enumerate(rows):
        url = (row.get(COL_RECORDING) or "").strip()
        existing = (row.get(COL_TRANSCRIPTION) or "").strip()
        try:
            duration = int(row.get(COL_DURATION, "0") or 0)
        except (ValueError, TypeError):
            duration = 0
        outcome = (row.get(COL_OUTCOME) or "").strip().lower().replace("result: ", "").strip()

        if not url:
            stats["no_url"] += 1
            continue
        if outcome in skip_outcomes:
            stats["voicemail"] += 1
            continue
        if skip_existing_transcript and existing and not existing.startswith("[TRANSCRIPTION_ERROR"):
            stats["has_transcript"] += 1
            continue
        if duration < min_duration:
            stats["below_duration"] += 1
            continue
        to_transcribe.append((i, row))
    return to_transcribe, stats


# ---------------------------------------------------------------------------
# TranscriptProcessor: orchestration + optional Mistral summary
# ---------------------------------------------------------------------------


class TranscriptProcessor:
    """Transcribe call recordings via Mistral Voxtral, sync to Attio; optional summarise transcript text."""

    def __init__(self, mistral_api_key: str = "", attio_api_key: str = ""):
        self.mistral_api_key = mistral_api_key or ""
        self.attio_api_key = attio_api_key or ""
        self._mistral_client: Optional[Mistral] = None

    def _get_mistral_client(self) -> Optional[Mistral]:
        if not self.mistral_api_key:
            return None
        if self._mistral_client is None:
            self._mistral_client = Mistral(api_key=self.mistral_api_key)
        return self._mistral_client

    def _call_mistral(self, transcript_text: str) -> dict:
        """Call Mistral chat for summary. Returns {summary, clean_transcript, action_items, sentiment}."""
        if not self.mistral_api_key or not (transcript_text or "").strip():
            return {"summary": "", "clean_transcript": transcript_text, "action_items": "", "sentiment": ""}
        try:
            with httpx.Client(timeout=60.0) as client:
                resp = client.post(
                    "https://api.mistral.ai/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {self.mistral_api_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": "mistral-small-latest",
                        "messages": [
                            {
                                "role": "user",
                                "content": (
                                    "Summarise this call transcript in 2-3 sentences. "
                                    "Then list any action items. End with one word: sentiment (positive/neutral/negative). "
                                    "Format: SUMMARY: ... ACTIONS: ... SENTIMENT: ..."
                                ),
                            },
                            {"role": "user", "content": (transcript_text or "")[:15000]},
                        ],
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                content = (data.get("choices") or [{}])[0].get("message", {}).get("content", "") or ""
                return {"summary": content, "clean_transcript": transcript_text, "action_items": "", "sentiment": ""}
        except Exception as e:
            logger.warning("Mistral summary failed: %s", e)
            return {"summary": "", "clean_transcript": transcript_text, "action_items": "", "sentiment": ""}

    def write_to_attio(self, justcall_call_record_id: str, data: dict) -> None:
        """PATCH justcall_call with call_transcription, call_summary, ai_insights (for summarise path)."""
        if not self.attio_api_key:
            return
        payload = {"data": {"values": {}}}
        if data.get("clean_transcript"):
            payload["data"]["values"]["call_transcription"] = [{"value": (data["clean_transcript"] or "")[:50000]}]
        if data.get("summary"):
            payload["data"]["values"]["call_summary"] = [{"value": (data["summary"] or "")[:10000]}]
        if data.get("summary"):
            payload["data"]["values"]["ai_insights"] = [{"value": (data["summary"] or "")[:10000]}]
        if not payload["data"]["values"]:
            return
        try:
            with httpx.Client(timeout=30.0) as client:
                client.patch(
                    f"{ATTIO_API_BASE}/objects/{ATTIO_OBJECT_JUSTCALL_CALL}/records/{justcall_call_record_id}",
                    headers={
                        "Authorization": f"Bearer {self.attio_api_key}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                )
        except Exception as e:
            logger.warning("Attio PATCH failed: %s", e)

    def process_calls(self, call_records: list[dict]) -> list[dict]:
        """For each call record (with transcript or recording ref), summarise via Mistral, return results.
        Does not transcribe audio; use run_transcription_pipeline for recording URL → transcript → Attio.
        """
        results = []
        for rec in call_records:
            transcript = rec.get("call_transcription") or rec.get(COL_TRANSCRIPTION) or rec.get("transcript_text") or ""
            if not transcript and (rec.get("call_recording") or rec.get(COL_RECORDING)):
                transcript = "(Recording URL present - transcript not fetched)"
            data = self._call_mistral(transcript)
            record_id = rec.get("record_id") or rec.get("id") or rec.get(COL_RECORD_ID)
            if record_id:
                self.write_to_attio(str(record_id), data)
            results.append({**rec, **data})
        return results

    def run_transcription_pipeline(
        self,
        rows: list[dict],
        fieldnames: list[str],
        *,
        min_duration: int = 15,
        skip_outcomes: Optional[list] = None,
        skip_existing_transcript: bool = False,
        max_calls: Optional[int] = None,
        dry_run: bool = False,
        sync_attio: bool = True,
        link_all_calls_to_people: bool = True,
    ) -> dict:
        """
        Filter rows, transcribe via Voxtral, optionally PATCH Attio and link to People.
        Updates rows in place with COL_TRANSCRIPTION. Returns stats dict.
        """
        if skip_outcomes is None:
            skip_outcomes = SKIP_OUTCOMES_DEFAULT
        to_transcribe, filter_stats = filter_rows_for_transcription(
            rows,
            min_duration=min_duration,
            skip_outcomes=skip_outcomes,
            skip_existing_transcript=skip_existing_transcript,
        )
        if max_calls:
            to_transcribe = to_transcribe[:max_calls]

        total = len(to_transcribe)
        logger.info(
            "Starting transcription pipeline: %s call(s) to transcribe (dry_run=%s, sync_attio=%s)",
            total, dry_run, sync_attio,
        )

        client = None if dry_run else self._get_mistral_client()
        stats = {
            "filter": filter_stats,
            "succeeded": 0,
            "failed": 0,
            "skipped": 0,
            "attio_updated": 0,
            "attio_failed": 0,
            "links_ok": 0,
            "links_fail": 0,
        }

        for idx, (row_idx, row) in enumerate(to_transcribe):
            url = row[COL_RECORDING].strip()
            rep_name = (row.get(COL_SETTER) or "").strip()
            record_id = (row.get(COL_RECORD_ID) or "").strip()
            people_id = (row.get(COL_PEOPLE_RECORD_ID) or "").strip()
            call_id = row.get(COL_CALL_ID, "?")
            contact = row.get(COL_CONTACT, "?")
            logger.info("[%s/%s] Transcribing call %s | %s -> %s", idx + 1, total, call_id, rep_name or "?", contact)

            if dry_run:
                transcript = "[DRY_RUN] Placeholder transcript (no API calls)."
            else:
                transcript = transcribe_url(client, url, rep_name=rep_name)

            if transcript and not transcript.startswith("[TRANSCRIPTION_ERROR"):
                rows[row_idx][COL_TRANSCRIPTION] = transcript
                stats["succeeded"] += 1
                if sync_attio and record_id and not dry_run and self.attio_api_key:
                    if update_attio_call_transcript(record_id, transcript, self.attio_api_key):
                        stats["attio_updated"] += 1
                        logger.info("  -> Synced transcript to Attio record %s", record_id[:12])
                    else:
                        stats["attio_failed"] += 1
                        logger.warning("  -> Attio PATCH failed for %s", record_id[:12])
            elif transcript.startswith("[TRANSCRIPTION_ERROR"):
                rows[row_idx][COL_TRANSCRIPTION] = transcript
                stats["failed"] += 1
                logger.warning("  -> Transcription failed: %s", transcript[:80])
            else:
                stats["skipped"] += 1
                logger.info("  -> Empty transcript, skipped")

            if idx < len(to_transcribe):
                time.sleep(0.5)

        # Link all call records to people (including those skipped for transcription)
        if sync_attio and link_all_calls_to_people and self.attio_api_key and not dry_run:
            to_link = [
                (r.get(COL_RECORD_ID, "").strip(), r.get(COL_PEOPLE_RECORD_ID, "").strip())
                for r in rows
                if (r.get(COL_RECORD_ID) or "").strip()
                and (r.get(COL_PEOPLE_RECORD_ID) or "").strip()
                and not r.get("_already_linked", False)
            ]
            logger.info("Linking %s call(s) to People in Attio", len(to_link))
            for rid, pid in to_link:
                if link_attio_call_to_people(rid, pid, self.attio_api_key):
                    stats["links_ok"] += 1
                else:
                    stats["links_fail"] += 1
                time.sleep(0.2)

        logger.info(
            "Pipeline complete: succeeded=%s failed=%s skipped=%s attio_updated=%s links_ok=%s",
            stats["succeeded"], stats["failed"], stats["skipped"],
            stats["attio_updated"], stats["links_ok"],
        )
        return stats
