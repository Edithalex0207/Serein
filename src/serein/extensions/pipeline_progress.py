"""Backlog, throughput and a conservative ETA for the event pipeline.

The progress report is intentionally read-only and never fabricates a
countdown. An ETA is returned only when the recent samples are fresh, span a
wide enough window and show real progress; otherwise ``eta_reason`` explains
why an estimate would be misleading (cold start, pause, throttling, stall).
"""
from datetime import datetime, timedelta, timezone

from ..core.store import Store

# Sliding window used to measure throughput.
MIN_SAMPLES = 3
MIN_WINDOW_SECONDS = 60.0
WINDOW_SECONDS = 1800.0
# No successful advance within this age means the pipeline is paused/blocked.
STALE_SAMPLE_SECONDS = 300.0
# Keep the sample table bounded.
KEEP_RECENT_SAMPLES = 200


def create_samples_table(conn):
    conn.execute(
        """CREATE TABLE IF NOT EXISTS pipeline_progress_samples(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sampled_at TEXT NOT NULL,
            stage TEXT NOT NULL,
            completed_count INTEGER NOT NULL,
            total_count INTEGER NOT NULL)"""
    )


def _table_exists(conn, name):
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def counts(store):
    """Return total raw events and the subset with a durable route."""
    if not _table_exists(store.conn, "raw_events"):
        return {"total": 0, "completed": 0, "pending": 0}
    total = store.conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]
    if _table_exists(store.conn, "pipeline_route_provenance"):
        completed = store.conn.execute(
            """SELECT COUNT(DISTINCT raw_id) FROM (
                SELECT raw_id FROM pipeline_routes
                UNION
                SELECT raw_id FROM pipeline_route_provenance)"""
        ).fetchone()[0]
    elif _table_exists(store.conn, "pipeline_routes"):
        completed = store.conn.execute(
            "SELECT COUNT(DISTINCT raw_id) FROM pipeline_routes"
        ).fetchone()[0]
    else:
        completed = 0
    completed = min(completed, total)
    return {"total": total, "completed": completed, "pending": total - completed}


def stage_for(current, result_status):
    if current["total"] == 0:
        return "empty"
    if current["pending"] == 0:
        return "done"
    if result_status in ("paused", "blocked"):
        return "blocked"
    # Routing is the dominant, measurable phase for large imports.
    return "routing"


def record_sample(database, result_status=""):
    """Append one progress sample after an advance; safe to call on every run."""
    with Store(database) as store:
        create_samples_table(store.conn)
        current = counts(store)
        stage = stage_for(current, result_status)
        store.conn.execute(
            """INSERT INTO pipeline_progress_samples
                (sampled_at, stage, completed_count, total_count)
                VALUES (?,?,?,?)""",
            (
                datetime.now(timezone.utc).isoformat(),
                stage,
                current["completed"],
                current["total"],
            ),
        )
        store.conn.execute(
            """DELETE FROM pipeline_progress_samples WHERE id NOT IN (
                SELECT id FROM pipeline_progress_samples
                ORDER BY id DESC LIMIT ?)""",
            (KEEP_RECENT_SAMPLES,),
        )


def _parse(value):
    return datetime.fromisoformat(value)


def _read_samples(store):
    if not _table_exists(store.conn, "pipeline_progress_samples"):
        return []
    rows = store.conn.execute(
        """SELECT sampled_at, stage, completed_count, total_count
            FROM pipeline_progress_samples
            ORDER BY id DESC LIMIT ?""",
        (MIN_SAMPLES * 4,),
    ).fetchall()
    return [dict(row) for row in reversed(rows)]


def estimate(samples, pending, now):
    """Measure throughput over the sliding window; return a conservative ETA."""
    if not samples:
        return None, "insufficient_samples", 0
    latest = samples[-1]
    latest_time = _parse(latest["sampled_at"])
    if (now - latest_time).total_seconds() > STALE_SAMPLE_SECONDS:
        return None, "paused_or_throttled", 0
    # Walk back to the oldest sample still inside the window. Samples are
    # ascending in time, so the first one inside the window is the start.
    start = latest
    for item in samples:
        item_time = _parse(item["sampled_at"])
        if (latest_time - item_time).total_seconds() <= WINDOW_SECONDS:
            start = item
            break
    span = (latest_time - _parse(start["sampled_at"])).total_seconds()
    usable = [
        item for item in samples
        if _parse(start["sampled_at"]) <= _parse(item["sampled_at"]) <= latest_time
    ]
    if len(usable) < MIN_SAMPLES or span < MIN_WINDOW_SECONDS:
        return None, "insufficient_samples", 0
    completed_delta = latest["completed_count"] - start["completed_count"]
    if completed_delta <= 0 or span <= 0:
        return None, "stalled", 0
    rate = completed_delta / span  # routed messages per second
    if pending <= 0:
        return 0, None, rate
    return pending / rate, None, rate


def _humanize(seconds):
    seconds = int(round(seconds))
    if seconds < 60:
        return f"{seconds} 秒"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes} 分 {seconds} 秒"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours} 小时 {minutes} 分"
    days, hours = divmod(hours, 24)
    return f"{days} 天 {hours} 小时"


def progress_report(database, result_status=""):
    """Return a JSON-friendly backlog / throughput / ETA report."""
    now = datetime.now(timezone.utc)
    with Store(database, read_only=True) as store:
        current = counts(store)
        samples = _read_samples(store)
    stage = stage_for(current, result_status)
    if stage == "done":
        eta_seconds, reason, rate = 0, None, 0
    elif stage == "empty":
        eta_seconds, reason, rate = None, "insufficient_samples", 0
    else:
        eta_seconds, reason, rate = estimate(samples, current["pending"], now)
    percent = (
        round(current["completed"] * 100.0 / current["total"], 2)
        if current["total"]
        else 0.0
    )
    return {
        "stage": stage,
        "total": current["total"],
        "completed": current["completed"],
        "pending": current["pending"],
        "percent": percent,
        "throughput_per_hour": round(rate * 3600.0, 1) if rate > 0 else None,
        "samples": len(samples),
        "eta_seconds": round(eta_seconds, 1) if eta_seconds is not None else None,
        "eta_human": "已完成" if stage == "done"
        else (_humanize(eta_seconds) if eta_seconds is not None else None),
        "eta_reliable": eta_seconds is not None,
        "eta_reason": reason,
    }
