#!/usr/bin/env python3
"""
Unit tests for process_inventory_queue (Phase A + Phase B).

Uses mocks for the DB connection and Shopify API so the test
runs without external dependencies.
"""

import json
import os
import sys
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.lib.process_inventory_sync import (
    _utc_date_from_shopify_updated_at,
    _dedupe_adjustment_events,
    _insert_history_from_queue,
    process_inventory_queue,
)


@pytest.fixture(autouse=True)
def _shop_domain(monkeypatch):
    # process_inventory_queue saute la queue si aucun domaine n'est défini
    monkeypatch.setenv("SHOPIFY_STORE_DOMAIN", "test-store.myshopify.com")


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

class TestUtcDateFromShopifyUpdatedAt:

    def test_none(self):
        assert _utc_date_from_shopify_updated_at(None) is None

    def test_empty_string(self):
        assert _utc_date_from_shopify_updated_at("") is None

    def test_iso_zulu(self):
        assert _utc_date_from_shopify_updated_at("2026-04-04T22:30:00Z") == date(2026, 4, 4)

    def test_iso_offset(self):
        # 2026-04-05T01:30:00+05:00 == 2026-04-04T20:30:00 UTC
        assert _utc_date_from_shopify_updated_at("2026-04-05T01:30:00+05:00") == date(2026, 4, 4)

    def test_datetime_object(self):
        dt = datetime(2026, 3, 31, 23, 0, 0, tzinfo=timezone.utc)
        assert _utc_date_from_shopify_updated_at(dt) == date(2026, 3, 31)


class TestDedupeAdjustmentEvents:

    def test_no_dupes(self):
        rows = [
            {"inventory_item_id": "1", "second": "ts1", "inventory_state": "a", "inventory_change_reason": "r", "reference_document_type": "d", "inventory_adjustment_change": "5"},
            {"inventory_item_id": "1", "second": "ts2", "inventory_state": "a", "inventory_change_reason": "r", "reference_document_type": "d", "inventory_adjustment_change": "5"},
        ]
        assert len(_dedupe_adjustment_events(rows)) == 2

    def test_dedup(self):
        row = {"inventory_item_id": "1", "second": "ts1", "inventory_state": "a", "inventory_change_reason": "r", "reference_document_type": "d", "inventory_adjustment_change": "5"}
        assert len(_dedupe_adjustment_events([row, row.copy()])) == 1


# ---------------------------------------------------------------------------
# _insert_history_from_queue
# ---------------------------------------------------------------------------

class TestInsertHistoryFromQueue:

    def _make_cursor_and_conn(self, inv_row=None):
        cur = MagicMock()
        conn = MagicMock()
        cur.fetchone.return_value = inv_row
        cur.rowcount = 1
        return cur, conn

    def test_insert_new_item(self):
        cur, conn = self._make_cursor_and_conn(inv_row=None)
        qty = {"available": 10, "committed": 2, "on_hand": 12, "incoming": 0, "reserved": 0}
        ctx = {"data_source": "US", "company_code": "AL", "commercial_organisation": "US"}
        ts = datetime(2026, 4, 5, 10, 0, 0, tzinfo=timezone.utc)

        result = _insert_history_from_queue(cur, conn, 111, 222, qty, ts, ctx)
        assert result == 1
        assert cur.execute.call_count == 2  # SELECT + INSERT

    def test_insert_existing_item_computes_delta(self):
        # old available = 15, new = 10 => movement = -5
        old_row = (15, 3, 0, 5, 0, 1, 0, 900, 800, "SKU-1")
        cur, conn = self._make_cursor_and_conn(inv_row=old_row)
        qty = {"available": 10, "committed": 2, "on_hand": 12, "incoming": 0, "reserved": 0}
        ctx = {"data_source": "US", "company_code": "AL", "commercial_organisation": "US"}
        ts = "2026-04-05T10:00:00Z"

        result = _insert_history_from_queue(cur, conn, 111, 222, qty, ts, ctx)
        assert result == 1

        insert_call = cur.execute.call_args_list[1]
        args = insert_call[0][1]
        avail_movement_idx = 13  # position of avail_movement in params
        assert args[avail_movement_idx] == -5

    def test_dedup_returns_zero(self):
        cur, conn = self._make_cursor_and_conn(inv_row=None)
        cur.rowcount = 0  # nothing inserted (dedup WHERE NOT EXISTS)
        qty = {"available": 10, "committed": 0, "on_hand": 10, "incoming": 0, "reserved": 0}
        ctx = {"data_source": "US", "company_code": "AL", "commercial_organisation": "US"}

        result = _insert_history_from_queue(cur, conn, 111, 222, qty, None, ctx)
        assert result == 0


# ---------------------------------------------------------------------------
# process_inventory_queue (full flow)
# ---------------------------------------------------------------------------

FAKE_QUEUE_ROW = (
    1,                                                   # queue_id
    44387774693447,                                      # inventory_item_id
    65446314055,                                         # location_id
    json.dumps({"available": 8, "committed": 1, "on_hand": 9, "incoming": 0, "reserved": 0}),
    "2026-04-04T22:30:00Z",                              # shopify_updated_at
)


@patch("api.lib.process_inventory_sync._pg_connect")
@patch("api.lib.process_inventory_sync.get_store_context", return_value={
    "data_source": "US", "company_code": "AL", "commercial_organisation": "US"})
@patch("api.lib.process_inventory_sync._insert_history_from_queue", return_value=1)
def test_phase_a_upserts_and_inserts_history(mock_hist, mock_ctx, mock_connect):
    """Phase A should UPSERT inventory AND insert a WEBHOOK row in history."""
    cur = MagicMock()
    conn = MagicMock()
    mock_connect.return_value = conn
    conn.cursor.return_value = cur

    # First fetchall = pending rows, second = enrichment rows (empty)
    cur.fetchall.side_effect = [
        [FAKE_QUEUE_ROW],
        [],  # no enrichment rows
    ]
    cur.fetchone.return_value = (False,)  # was_inserted = False (updated)

    stats = process_inventory_queue()

    assert stats["updated"] == 1
    assert stats["history_inserted"] == 1
    mock_hist.assert_called_once()

    # Phase A laisse history_synced = FALSE jusqu'à enrichissement Phase B
    update_calls = [c for c in cur.execute.call_args_list if "history_synced = FALSE" in str(c)]
    assert len(update_calls) >= 1


@patch("api.lib.process_inventory_sync._pg_connect")
@patch("api.lib.process_inventory_sync.get_store_context", return_value={
    "data_source": "US", "company_code": "AL", "commercial_organisation": "US"})
@patch("api.lib.process_inventory_sync._insert_history_from_queue", return_value=1)
def test_phase_a_failure_marks_queue_failed(mock_hist, mock_ctx, mock_connect):
    """When UPSERT raises, queue row should end up as 'failed'."""
    cur = MagicMock()
    conn = MagicMock()
    mock_connect.return_value = conn
    conn.cursor.return_value = cur

    def side_effect_execute(sql, params=None):
        if "INSERT INTO inventory" in sql:
            raise RuntimeError("db error")
    cur.execute.side_effect = side_effect_execute
    cur.fetchall.side_effect = [[FAKE_QUEUE_ROW], []]

    stats = process_inventory_queue()

    assert stats["failed"] == 1
    assert len(stats["errors"]) == 1


@patch("api.lib.shopifyql_helpers.fetch_all_locations",
       return_value={"65446314055": "Main Warehouse"})
@patch("api.lib.shopifyql_helpers.fetch_adjustments_for_pair",
       return_value=[{"inventory_item_id": "44387774693447",
                      "second": "2026-04-04T22:30:00Z",
                      "inventory_state": "available",
                      "inventory_change_reason": "correction",
                      "reference_document_type": "",
                      "inventory_adjustment_change": "-2"}])
@patch("api.lib.process_inventory_sync._enrich_webhook_rows")
@patch("api.lib.process_inventory_sync._pg_connect")
@patch("api.lib.process_inventory_sync.get_store_context", return_value={
    "data_source": "US", "company_code": "AL", "commercial_organisation": "US"})
@patch("api.lib.process_inventory_sync._insert_history_from_queue", return_value=1)
def test_phase_b_enriches_old_rows(mock_hist, mock_ctx, mock_connect,
                                    mock_enrich, mock_fetch_adj, mock_locs):
    """Phase B picks up completed rows older than 30 min with history_synced=FALSE."""
    cur = MagicMock()
    conn = MagicMock()
    mock_connect.return_value = conn
    conn.cursor.return_value = cur

    # Phase B interroge le jour de l'update ET aujourd'hui : l'événement n'existe que le jour de l'update
    events = mock_fetch_adj.return_value
    mock_fetch_adj.side_effect = lambda item_id, loc_name, d: events if d == date(2026, 4, 4) else []
    mock_enrich.return_value = 1

    enrichment_row = (44387774693447, 65446314055, "Main Warehouse", "2026-04-04T22:30:00Z", None)
    cur.fetchall.side_effect = [
        [],               # no pending rows
        [enrichment_row], # one enrichment row
    ]

    stats = process_inventory_queue()

    assert stats["enriched"] == 1
    mock_enrich.assert_called_once()
    mock_fetch_adj.assert_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
