"""
SQLite data layer for leads. Replaces Baserow as single source of truth.
"""
import json
import os
import sqlite3
from contextlib import contextmanager
from typing import Any, Optional

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS leads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    domains         TEXT,
    listing_url     TEXT,
    website_url     TEXT,
    lead_source     TEXT NOT NULL DEFAULT 'directory',
    status          TEXT NOT NULL DEFAULT 'pending_review',

    office_phone    TEXT,
    office_email    TEXT,
    description     TEXT,
    edited_description TEXT,

    street_address       TEXT,
    primary_location_line_1    TEXT,
    primary_location_locality  TEXT,
    primary_location_region    TEXT,
    primary_location_postcode  TEXT,

    segment              TEXT,
    organisational_structure TEXT,
    areas_of_accountancy TEXT,

    decision_makers      TEXT,
    associated_emails    TEXT,
    associated_mobiles   TEXT,
    associated_info      TEXT,
    linkedin             TEXT,
    facebook             TEXT,
    confidence_score     REAL DEFAULT 0.0,
    out_of_scope         INTEGER DEFAULT 0,
    out_of_scope_reason  TEXT,

    flag_reason          TEXT,
    flag_source          TEXT,

    attio_status         TEXT DEFAULT 'new',
    attio_record_id      TEXT,
    attio_person_id      TEXT,
    duplicate_of         TEXT,

    justcall_campaign_id TEXT,
    lead_grade           TEXT,
    is_staged            INTEGER DEFAULT 0,

    created_at           TEXT DEFAULT (datetime('now')),
    updated_at           TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);
CREATE INDEX IF NOT EXISTS idx_leads_source ON leads(lead_source);
CREATE INDEX IF NOT EXISTS idx_leads_attio ON leads(attio_status);

CREATE TABLE IF NOT EXISTS campaign_lists (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    name                 TEXT NOT NULL,
    attio_sync_status    TEXT DEFAULT 'draft',
    justcall_campaign_id TEXT,
    attio_list_id        TEXT,
    attio_object         TEXT DEFAULT 'companies',
    field_mapping        TEXT,
    created_at           TEXT DEFAULT (datetime('now')),
    updated_at           TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS campaign_list_members (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_list_id INTEGER NOT NULL REFERENCES campaign_lists(id) ON DELETE CASCADE,
    lead_id          INTEGER NOT NULL,
    added_at         TEXT DEFAULT (datetime('now')),
    UNIQUE(campaign_list_id, lead_id)
);

CREATE INDEX IF NOT EXISTS idx_clm_list ON campaign_list_members(campaign_list_id);
CREATE INDEX IF NOT EXISTS idx_clm_lead ON campaign_list_members(lead_id);

CREATE TABLE IF NOT EXISTS custom_tags (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    name             TEXT NOT NULL UNIQUE,
    created_at       TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS lead_tags (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id          INTEGER NOT NULL,
    tag_id           INTEGER NOT NULL REFERENCES custom_tags(id) ON DELETE CASCADE,
    added_at         TEXT DEFAULT (datetime('now')),
    UNIQUE(lead_id, tag_id)
);

CREATE INDEX IF NOT EXISTS idx_lead_tags_lead ON lead_tags(lead_id);
CREATE INDEX IF NOT EXISTS idx_lead_tags_tag ON lead_tags(tag_id);
"""

# Columns that accept JSON (serialize on write, deserialize on read)
JSON_COLUMNS = frozenset({
    "decision_makers", "associated_emails", "associated_mobiles", "areas_of_accountancy"
})

# All writable columns (excluding id, created_at, updated_at).
# Valid statuses (no enum enforcement): pending_review, enriched, ready_for_attio,
# synced_to_attio, duplicate, flagged_keyword, flagged_llm, excluded.
LEAD_COLUMNS = frozenset({
    "name", "domains", "listing_url", "website_url", "lead_source", "status",
    "office_phone", "office_email", "description", "edited_description",
    "street_address", "primary_location_line_1", "primary_location_locality",
    "primary_location_region", "primary_location_postcode",
    "segment", "organisational_structure", "areas_of_accountancy",
    "decision_makers", "associated_emails", "associated_mobiles", "associated_info",
    "linkedin", "facebook", "confidence_score", "out_of_scope", "out_of_scope_reason",
    "flag_reason", "flag_source",
    "attio_status", "attio_record_id", "attio_person_id", "duplicate_of",
    "justcall_campaign_id", "lead_grade", "is_staged",
})


def _serialize_value(key: str, value: Any) -> Any:
    if key in JSON_COLUMNS and value is not None:
        if isinstance(value, (list, dict)):
            return json.dumps(value)
        if isinstance(value, str) and value.strip():
            try:
                json.loads(value)
                return value
            except (json.JSONDecodeError, TypeError):
                return json.dumps([value]) if value else None
    return value


def _row_to_dict(row: sqlite3.Row) -> dict:
    d = dict(row)
    for key in JSON_COLUMNS:
        if key in d and d[key] is not None and isinstance(d[key], str):
            try:
                d[key] = json.loads(d[key])
            except (json.JSONDecodeError, TypeError):
                pass
    return d


class LeadDB:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or os.getenv("DB_PATH", "leads.db")
        self._init_db()

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(SCHEMA_SQL)
            # Migration: add attio_person_id if missing (existing DBs)
            try:
                conn.execute("ALTER TABLE leads ADD COLUMN attio_person_id TEXT")
            except sqlite3.OperationalError:
                pass  # column already exists
            try:
                conn.execute("ALTER TABLE leads ADD COLUMN flag_reason TEXT")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE leads ADD COLUMN flag_source TEXT")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE leads ADD COLUMN is_staged INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE campaign_lists ADD COLUMN attio_object TEXT DEFAULT 'companies'")
            except sqlite3.OperationalError:
                pass

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def insert_lead(self, data: dict) -> int:
        """Insert a lead. JSON columns are serialized. Returns new row id."""
        filtered = {k: _serialize_value(k, v) for k, v in data.items() if k in LEAD_COLUMNS}
        cols = ", ".join(filtered.keys())
        placeholders = ", ".join("?" for _ in filtered)
        with self._conn() as conn:
            conn.execute(
                f"INSERT INTO leads ({cols}) VALUES ({placeholders})",
                list(filtered.values()),
            )
            return conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    def update_lead(self, lead_id: int, data: dict) -> None:
        """Update a lead. Sets updated_at. JSON columns are serialized."""
        filtered = {k: _serialize_value(k, v) for k, v in data.items() if k in LEAD_COLUMNS}
        if not filtered:
            return
        set_parts = [f"{k} = ?" for k in filtered]
        set_parts.append("updated_at = datetime('now')")
        set_clause = ", ".join(set_parts)
        values = list(filtered.values()) + [lead_id]
        with self._conn() as conn:
            conn.execute(f"UPDATE leads SET {set_clause} WHERE id = ?", values)

    def get_leads(
        self,
        status: Optional[str] = None,
        lead_source: Optional[str] = None,
        limit: int = 500,
    ) -> list[dict]:
        """Return leads as list of dicts. Optional filters by status and lead_source."""
        conditions = []
        params = []
        if status is not None:
            conditions.append("status = ?")
            params.append(status)
        if lead_source is not None:
            conditions.append("lead_source = ?")
            params.append(lead_source)
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        params.append(limit)
        with self._conn() as conn:
            cur = conn.execute(
                f"SELECT * FROM leads {where} ORDER BY id LIMIT ?",
                params,
            )
            return [_row_to_dict(r) for r in cur.fetchall()]

    def get_leads_by_ids(self, lead_ids: list[int]) -> list[dict]:
        """Return leads whose id is in the given list. Order by id."""
        if not lead_ids:
            return []
        placeholders = ", ".join("?" for _ in lead_ids)
        with self._conn() as conn:
            cur = conn.execute(
                f"SELECT * FROM leads WHERE id IN ({placeholders}) ORDER BY id",
                lead_ids,
            )
            return [_row_to_dict(r) for r in cur.fetchall()]

    def get_staged_lead_ids(self) -> list[int]:
        """Return lead ids where is_staged = 1. Used to persist staging across sessions."""
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT id FROM leads WHERE is_staged = 1 ORDER BY id",
            )
            return [r[0] for r in cur.fetchall()]

    def get_leads_by_statuses(
        self,
        statuses: list[str],
        lead_source: Optional[str] = None,
        limit: int = 500,
    ) -> list[dict]:
        """Return leads whose status is in the given list. Optional lead_source filter."""
        if not statuses:
            return []
        conditions = ["status IN (" + ", ".join("?" for _ in statuses) + ")"]
        params: list[Any] = list(statuses)
        if lead_source is not None:
            conditions.append("lead_source = ?")
            params.append(lead_source)
        params.append(limit)
        where = "WHERE " + " AND ".join(conditions)
        with self._conn() as conn:
            cur = conn.execute(
                f"SELECT * FROM leads {where} ORDER BY id LIMIT ?",
                params,
            )
            return [_row_to_dict(r) for r in cur.fetchall()]

    def bulk_update_status(
        self,
        lead_ids: list[int],
        status: str,
        extra: Optional[dict] = None,
    ) -> None:
        """Batch update status (and optional extra fields) for given lead ids."""
        if not lead_ids or not (status and str(status).strip()):
            return
        updates: dict[str, Any] = {"status": status}
        if extra:
            for k, v in extra.items():
                if k in LEAD_COLUMNS:
                    updates[k] = _serialize_value(k, v)
        set_parts = [f"{k} = ?" for k in updates]
        set_parts.append("updated_at = datetime('now')")
        set_clause = ", ".join(set_parts)
        values = list(updates.values())
        placeholders = ", ".join("?" for _ in lead_ids)
        with self._conn() as conn:
            conn.execute(
                f"UPDATE leads SET {set_clause} WHERE id IN ({placeholders})",
                values + lead_ids,
            )

    def count_by_flag_source(self) -> dict[str, int]:
        """Return counts per flag_source where flag_source IS NOT NULL."""
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT flag_source, COUNT(*) FROM leads WHERE flag_source IS NOT NULL GROUP BY flag_source"
            )
            return {row[0]: row[1] for row in cur.fetchall()}

    def get_lead(self, lead_id: int) -> Optional[dict]:
        """Return a single lead by id or None."""
        with self._conn() as conn:
            cur = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,))
            row = cur.fetchone()
            return _row_to_dict(row) if row else None

    def count_by_status(self) -> dict[str, int]:
        """Return counts per status."""
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT status, COUNT(*) FROM leads GROUP BY status"
            )
            return {row[0]: row[1] for row in cur.fetchall()}

    def count_by_source(self) -> dict[str, int]:
        """Return counts per lead_source."""
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT lead_source, COUNT(*) FROM leads GROUP BY lead_source"
            )
            return {row[0]: row[1] for row in cur.fetchall()}

    def get_leads_by_campaign(self, campaign_id: str, limit: int = 500) -> list[dict]:
        """Return leads in the given JustCall campaign. Order by id."""
        if not campaign_id:
            return []
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT * FROM leads WHERE justcall_campaign_id = ? ORDER BY id LIMIT ?",
                (str(campaign_id), limit),
            )
            return [_row_to_dict(r) for r in cur.fetchall()]

    def count_by_campaign(self) -> dict[str, int]:
        """Return counts per justcall_campaign_id where not null."""
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT justcall_campaign_id, COUNT(*) FROM leads WHERE justcall_campaign_id IS NOT NULL GROUP BY justcall_campaign_id"
            )
            return {str(row[0]): row[1] for row in cur.fetchall()}

    def search_by_domain(self, domain: str) -> list[dict]:
        """Return leads whose domains field matches (case-insensitive)."""
        if not domain:
            return []
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT * FROM leads WHERE LOWER(domains) = LOWER(?) ORDER BY id",
                (domain.strip(),),
            )
            return [_row_to_dict(r) for r in cur.fetchall()]

    def get_lead_by_attio_record_id(self, attio_record_id: str) -> Optional[dict]:
        """Return first lead with this attio_record_id, or None."""
        if not attio_record_id:
            return None
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT * FROM leads WHERE attio_record_id = ? LIMIT 1",
                (attio_record_id.strip(),),
            )
            row = cur.fetchone()
            return _row_to_dict(row) if row else None

    def get_lead_by_attio_person_id(self, attio_person_id: str) -> Optional[dict]:
        """Return first lead with this attio_person_id, or None."""
        if not attio_person_id:
            return None
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT * FROM leads WHERE attio_person_id = ? LIMIT 1",
                (attio_person_id.strip(),),
            )
            row = cur.fetchone()
            return _row_to_dict(row) if row else None

    # --- Campaign Lists ---

    def create_campaign_list(
        self,
        name: str,
        field_mapping: Optional[list] = None,
        attio_object: str = "companies",
    ) -> int:
        """Create a campaign list. Returns new list id. attio_object is 'companies' or 'people'."""
        mapping_json = json.dumps(field_mapping) if field_mapping else None
        obj = "people" if attio_object == "people" else "companies"
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO campaign_lists (name, field_mapping, attio_object) VALUES (?, ?, ?)",
                (name, mapping_json, obj),
            )
            return conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    def get_campaign_lists(self) -> list[dict]:
        """Return all campaign lists ordered by most recent first."""
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT cl.*, COUNT(clm.id) AS member_count "
                "FROM campaign_lists cl "
                "LEFT JOIN campaign_list_members clm ON cl.id = clm.campaign_list_id "
                "GROUP BY cl.id ORDER BY cl.id DESC"
            )
            rows = []
            for r in cur.fetchall():
                d = dict(r)
                if d.get("field_mapping") and isinstance(d["field_mapping"], str):
                    try:
                        d["field_mapping"] = json.loads(d["field_mapping"])
                    except (json.JSONDecodeError, TypeError):
                        pass
                rows.append(d)
            return rows

    def get_campaign_list(self, list_id: int) -> Optional[dict]:
        """Return a single campaign list by id, or None."""
        with self._conn() as conn:
            cur = conn.execute("SELECT * FROM campaign_lists WHERE id = ?", (list_id,))
            row = cur.fetchone()
            if not row:
                return None
            d = dict(row)
            if d.get("field_mapping") and isinstance(d["field_mapping"], str):
                try:
                    d["field_mapping"] = json.loads(d["field_mapping"])
                except (json.JSONDecodeError, TypeError):
                    pass
            return d

    def update_campaign_list(self, list_id: int, data: dict) -> None:
        """Update campaign list fields. Accepts name, attio_sync_status, justcall_campaign_id, attio_list_id, attio_object, field_mapping."""
        allowed = {"name", "attio_sync_status", "justcall_campaign_id", "attio_list_id", "attio_object", "field_mapping"}
        filtered = {}
        for k, v in data.items():
            if k not in allowed:
                continue
            if k == "field_mapping" and isinstance(v, (list, dict)):
                filtered[k] = json.dumps(v)
            else:
                filtered[k] = v
        if not filtered:
            return
        set_parts = [f"{k} = ?" for k in filtered]
        set_parts.append("updated_at = datetime('now')")
        values = list(filtered.values()) + [list_id]
        with self._conn() as conn:
            conn.execute(
                f"UPDATE campaign_lists SET {', '.join(set_parts)} WHERE id = ?",
                values,
            )

    def delete_campaign_list(self, list_id: int) -> None:
        """Delete a campaign list and its members."""
        with self._conn() as conn:
            conn.execute("DELETE FROM campaign_list_members WHERE campaign_list_id = ?", (list_id,))
            conn.execute("DELETE FROM campaign_lists WHERE id = ?", (list_id,))

    def add_to_campaign_list(self, list_id: int, lead_ids: list[int]) -> int:
        """Add leads to a campaign list. Skips duplicates. Returns count added."""
        if not lead_ids:
            return 0
        added = 0
        with self._conn() as conn:
            for lid in lead_ids:
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO campaign_list_members (campaign_list_id, lead_id) VALUES (?, ?)",
                        (list_id, lid),
                    )
                    added += conn.execute("SELECT changes()").fetchone()[0]
                except sqlite3.IntegrityError:
                    pass
        return added

    def remove_from_campaign_list(self, list_id: int, lead_ids: list[int]) -> None:
        """Remove specific leads from a campaign list."""
        if not lead_ids:
            return
        placeholders = ", ".join("?" for _ in lead_ids)
        with self._conn() as conn:
            conn.execute(
                f"DELETE FROM campaign_list_members WHERE campaign_list_id = ? AND lead_id IN ({placeholders})",
                [list_id] + lead_ids,
            )

    def get_campaign_list_members(self, list_id: int, limit: int = 5000) -> list[dict]:
        """Return leads that are members of the given campaign list (joined)."""
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT l.* FROM leads l "
                "INNER JOIN campaign_list_members clm ON l.id = clm.lead_id "
                "WHERE clm.campaign_list_id = ? ORDER BY l.id LIMIT ?",
                (list_id, limit),
            )
            return [_row_to_dict(r) for r in cur.fetchall()]

    def get_campaign_list_member_ids(self, list_id: int) -> list[int]:
        """Return lead ids in a campaign list."""
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT lead_id FROM campaign_list_members WHERE campaign_list_id = ? ORDER BY lead_id",
                (list_id,),
            )
            return [r[0] for r in cur.fetchall()]

    def clear_campaign_list(self, list_id: int) -> None:
        """Remove all members from a campaign list."""
        with self._conn() as conn:
            conn.execute(
                "DELETE FROM campaign_list_members WHERE campaign_list_id = ?",
                (list_id,),
            )

    # --- Custom Tags ---

    def create_tag(self, name: str) -> int:
        """Create a custom tag. Returns tag id. Raises on duplicate."""
        with self._conn() as conn:
            conn.execute("INSERT INTO custom_tags (name) VALUES (?)", (name.strip(),))
            return conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    def get_or_create_tag(self, name: str) -> int:
        """Get existing tag id or create it. Returns tag id."""
        name = name.strip()
        with self._conn() as conn:
            row = conn.execute("SELECT id FROM custom_tags WHERE name = ?", (name,)).fetchone()
            if row:
                return row[0]
            conn.execute("INSERT INTO custom_tags (name) VALUES (?)", (name,))
            return conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    def get_all_tags(self) -> list[dict]:
        """Return all custom tags with lead counts."""
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT ct.id, ct.name, ct.created_at, COUNT(lt.id) AS lead_count "
                "FROM custom_tags ct "
                "LEFT JOIN lead_tags lt ON ct.id = lt.tag_id "
                "GROUP BY ct.id ORDER BY ct.name"
            )
            return [dict(r) for r in cur.fetchall()]

    def delete_tag(self, tag_id: int) -> None:
        """Delete a custom tag and all its lead associations."""
        with self._conn() as conn:
            conn.execute("DELETE FROM lead_tags WHERE tag_id = ?", (tag_id,))
            conn.execute("DELETE FROM custom_tags WHERE id = ?", (tag_id,))

    def tag_leads(self, lead_ids: list[int], tag_id: int) -> int:
        """Tag multiple leads with a tag. Skips duplicates. Returns count added."""
        if not lead_ids:
            return 0
        added = 0
        with self._conn() as conn:
            for lid in lead_ids:
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO lead_tags (lead_id, tag_id) VALUES (?, ?)",
                        (lid, tag_id),
                    )
                    added += conn.execute("SELECT changes()").fetchone()[0]
                except sqlite3.IntegrityError:
                    pass
        return added

    def untag_leads(self, lead_ids: list[int], tag_id: int) -> None:
        """Remove a tag from multiple leads."""
        if not lead_ids:
            return
        placeholders = ", ".join("?" for _ in lead_ids)
        with self._conn() as conn:
            conn.execute(
                f"DELETE FROM lead_tags WHERE tag_id = ? AND lead_id IN ({placeholders})",
                [tag_id] + lead_ids,
            )

    def get_lead_tags(self, lead_id: int) -> list[dict]:
        """Return tags for a single lead."""
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT ct.id, ct.name FROM custom_tags ct "
                "INNER JOIN lead_tags lt ON ct.id = lt.tag_id "
                "WHERE lt.lead_id = ? ORDER BY ct.name",
                (lead_id,),
            )
            return [dict(r) for r in cur.fetchall()]

    def get_leads_by_tag(self, tag_id: int, limit: int = 5000) -> list[dict]:
        """Return leads that have a specific tag."""
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT l.* FROM leads l "
                "INNER JOIN lead_tags lt ON l.id = lt.lead_id "
                "WHERE lt.tag_id = ? ORDER BY l.id LIMIT ?",
                (tag_id, limit),
            )
            return [_row_to_dict(r) for r in cur.fetchall()]

    def get_leads_by_tags(self, tag_ids: list[int], limit: int = 5000) -> list[dict]:
        """Return leads that have ANY of the given tags."""
        if not tag_ids:
            return []
        placeholders = ", ".join("?" for _ in tag_ids)
        with self._conn() as conn:
            cur = conn.execute(
                f"SELECT DISTINCT l.* FROM leads l "
                f"INNER JOIN lead_tags lt ON l.id = lt.lead_id "
                f"WHERE lt.tag_id IN ({placeholders}) ORDER BY l.id LIMIT ?",
                tag_ids + [limit],
            )
            return [_row_to_dict(r) for r in cur.fetchall()]

    def purge_leads(
        self,
        lead_ids: Optional[list[int]] = None,
        status: Optional[str] = None,
        lead_source: Optional[str] = None,
    ) -> int:
        """
        Delete leads and their campaign list memberships.
        - If lead_ids is set: delete those leads only.
        - Else if status and/or lead_source: delete leads matching the filter.
        - Else: delete all leads (full reset).
        Returns the number of leads deleted.
        """
        with self._conn() as conn:
            if lead_ids is not None:
                ids = lead_ids
            else:
                conditions = []
                params: list[Any] = []
                if status is not None:
                    conditions.append("status = ?")
                    params.append(status)
                if lead_source is not None:
                    conditions.append("lead_source = ?")
                    params.append(lead_source)
                where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
                cur = conn.execute(f"SELECT id FROM leads {where}", params)
                ids = [r[0] for r in cur.fetchall()]

            if not ids:
                return 0

            placeholders = ", ".join("?" for _ in ids)
            conn.execute(
                f"DELETE FROM lead_tags WHERE lead_id IN ({placeholders})",
                ids,
            )
            conn.execute(
                f"DELETE FROM campaign_list_members WHERE lead_id IN ({placeholders})",
                ids,
            )
            conn.execute(
                f"DELETE FROM leads WHERE id IN ({placeholders})",
                ids,
            )
            return len(ids)
