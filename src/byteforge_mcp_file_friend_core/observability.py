"""Bounded, capability-free service logging and authoritative scrape-time gauges."""

import json
import logging
import math
import os
import shutil
import time
from urllib.parse import urlsplit

from byteforge_loki_logging import configure_logging
from prometheus_client.core import GaugeMetricFamily

from byteforge_mcp_file_friend_core.database import Database

EVENTS = frozenset({"request_completed", "database_unavailable", "worker_pass_completed", "worker_pass_failed"})
METHODS = frozenset({"GET", "HEAD", "POST", "PATCH", "DELETE", "OPTIONS", "PUT", "OTHER"})
ROUTES = frozenset({"files", "hooks", "auth", "health", "ready", "metrics", "other"})
STATES = ("uploading", "verifying", "publishing", "ready", "deleting", "deleted", "expired", "failed")


class SafeRecord(logging.Filter):
    def filter(self, record):
        # Build a new record: traceback, message args and arbitrary extras never
        # enter either the stdout handler or the asynchronous Loki queue.
        known = record.name == "file_friend.events" and isinstance(record.msg, str) and record.msg in EVENTS
        if not known and record.levelno < logging.WARNING:
            return False
        safe = logging.LogRecord("file_friend.events", record.levelno, "", 0,
                                 record.msg if known else "dependency_error", (), None)
        if known:
            for key, allowed in (("method", METHODS), ("route", ROUTES)):
                value = getattr(record, key, None)
                if isinstance(value, str) and value in allowed:
                    setattr(safe, key, value)
            for key in ("status", "duration_seconds", "failures"):
                value = getattr(record, key, None)
                if type(value) in (int, float) and math.isfinite(value) and value >= 0:
                    setattr(safe, key, value)
        return safe


def configure_service_logging(application: str) -> None:
    local = os.environ.get("DEBUG_LOCAL", "true").lower() == "true"
    if not local:
        endpoint = urlsplit(os.environ.get("LOKI_ENDPOINT", ""))
        if (endpoint.scheme != "https" or not endpoint.hostname or endpoint.username
                or endpoint.password or endpoint.query or endpoint.fragment
                or endpoint.path != "/loki/api/v1/push"):
            raise ValueError("LOKI_ENDPOINT must be an HTTPS push endpoint without credentials or query")
        if os.environ.get("LOKI_CA_BUNDLE_PATH", "").lower() == "false":
            raise ValueError("Loki TLS verification must remain enabled")
    configure_logging(application_tag=application, debug_local=local,
                      local_level=os.environ.get("LOG_LEVEL", "INFO"), push_timeout=3.0)
    for handler in logging.getLogger().handlers:
        handler.addFilter(SafeRecord())
    # Clear framework handlers so none bypass the sanitized root sinks.
    for name in ("werkzeug", "gunicorn", "gunicorn.error", "gunicorn.access"):
        dependency = logging.getLogger(name)
        dependency.handlers.clear()
        dependency.propagate = True


def event(name: str, **fields) -> None:
    level = logging.ERROR if name in ("worker_pass_failed", "database_unavailable") else logging.INFO
    logging.getLogger("file_friend.events").log(level, name, extra=fields)


class LifecycleCollector:
    """One snapshot per scrape, not one gauge per Gunicorn worker or upload."""
    def __init__(self, config):
        self.config = config
        self.database = Database()

    @staticmethod
    def gauge(name, value, description):
        metric = GaugeMetricFamily(name, description)
        metric.add_metric([], value)
        return metric

    def collect(self):
        config = self.config
        yield self.gauge("ff_storage_budget_bytes", config.storage_budget, "Configured payload budget")
        yield self.gauge("ff_storage_reserve_bytes", config.min_free, "Configured free-space admission reserve")
        try:
            free = shutil.disk_usage(config.root).free
        except OSError:
            yield self.gauge("ff_storage_available", 0, "Filesystem statistics available")
        else:
            yield self.gauge("ff_storage_available", 1, "Filesystem statistics available")
            yield self.gauge("ff_storage_free_bytes", free, "Current filesystem free bytes")
        try:
            heartbeat = json.loads((config.root / "worker.json").read_text())
            age = time.time() - float(heartbeat["heartbeat"])
            failures = int(heartbeat["failures"])
            if not math.isfinite(age) or failures < 0:
                raise ValueError("Invalid heartbeat")
            valid = heartbeat["state"] == "running"
        except (OSError, ValueError, TypeError, KeyError):
            valid, age, failures = False, 0, 0
        yield self.gauge("ff_worker_healthy", int(valid and 0 <= age < 15 and failures < 3), "Worker health")
        yield self.gauge("ff_worker_heartbeat_age_seconds", max(0, age), "Heartbeat age; check health for missing heartbeat")
        yield self.gauge("ff_worker_consecutive_failures", failures, "Consecutive failed passes")
        try:
            with self.database.cursor() as cursor:
                cursor.execute("SELECT state, count(*) AS count, coalesce(sum(size),0) AS bytes, min(created_at) AS oldest "
                               "FROM uploads GROUP BY state")
                rows = {row["state"]: row for row in cursor.fetchall()}
        except Exception:  # noqa: BLE001 -- scrape failure must not leak SQL or disable health gauges.
            yield self.gauge("ff_database_up", 0, "Lifecycle database query succeeded")
            return
        yield self.gauge("ff_database_up", 1, "Lifecycle database query succeeded")
        counts = GaugeMetricFamily("ff_uploads", "Lifecycle records by bounded state", labels=["state"])
        for state in STATES:
            counts.add_metric([state], rows.get(state, {}).get("count", 0))
        yield counts
        ages = GaugeMetricFamily("ff_upload_oldest_age_seconds", "Oldest admission age by state", labels=["state"])
        for state in STATES[:5]:
            oldest = rows.get(state, {}).get("oldest")
            ages.add_metric([state], max(0, time.time() - oldest) if oldest is not None else 0)
        yield ages
        committed = sum(int(rows.get(state, {}).get("bytes", 0)) for state in STATES[:5])
        yield self.gauge("ff_storage_committed_bytes", committed, "Declared nonterminal payload bytes")
