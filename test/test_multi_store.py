#!/usr/bin/env python3
"""
Unit tests for the multi-store (US / JP / UK) isolation and onboarding changes.

Everything is mocked: no network, no database. Store context is driven by
monkeypatched env vars (COMMERCIAL_ORGANISATION, SHOPIFY_STORE_DOMAIN, ...).

Modules that read env at import time (process_inventory_sync reads
SHOPIFY_ACCESS_TOKEN / SHOPIFY_STORE_DOMAIN for its constants) are only
exercised through functions whose DB access (_pg_connect) is patched, and the
store-dependent helpers (get_store_context, get_current_shop_domain,
get_default_currency) read os.environ at call time, so monkeypatch.setenv is
effective for them.
"""

import json
import os
import re
import subprocess
import sys
import textwrap
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
import requests

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from api.lib import shopify_api
from api.lib.utils import get_current_shop_domain, get_default_currency
from api.lib.insert_order import extract_market_from_tags
from api.lib.process_draft_orders import (
    process_draft_order,
    process_draft_orders_delete_queue,
)
from api.lib.process_inventory_sync import process_inventory_queue
from api.lib.process_transactions import get_refund_details
from api.lib.product_processor import get_latest_product_update_date
from api.lib.location_processor import get_latest_location_date


STORE_ENV_VARS = (
    "SHOPIFY_ACCESS_TOKEN",
    "SHOPIFY_CLIENT_ID",
    "SHOPIFY_CLIENT_SECRET",
    "SHOPIFY_STORE_DOMAIN",
    "COMMERCIAL_ORGANISATION",
    "SHOP_CURRENCY",
)


@pytest.fixture(autouse=True)
def clean_store_env(monkeypatch):
    """Start every test with no store env, whatever the dev's .env contains.

    setenv-then-delenv makes monkeypatch record the original value, so vars
    that the code under test writes straight into os.environ are restored.
    """
    for name in STORE_ENV_VARS:
        monkeypatch.setenv(name, "")
        monkeypatch.delenv(name)


def _norm(sql):
    """Collapse whitespace so assertions do not depend on SQL formatting."""
    return re.sub(r"\s+", " ", sql).strip()


def _executed(cur):
    """Return [(normalised_sql, params)] for every cursor.execute call."""
    out = []
    for c in cur.execute.call_args_list:
        sql = c[0][0]
        params = c[0][1] if len(c[0]) > 1 else None
        out.append((_norm(sql), params))
    return out


def _mock_conn():
    cur = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


# ---------------------------------------------------------------------------
# 1. shopify_api.fetch_access_token / ensure_access_token
# ---------------------------------------------------------------------------

SECRET = "shpss_SUPER_SECRET_VALUE"


def _resp(status=200, body=None, text=""):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = body if body is not None else {}
    r.text = text
    return r


class TestFetchAccessToken:

    @patch("api.lib.shopify_api.requests.post")
    def test_posts_client_credentials_and_returns_token(self, mock_post):
        mock_post.return_value = _resp(200, {"access_token": "tok_123"})

        token = shopify_api.fetch_access_token("uk-shop.myshopify.com", "cid", SECRET)

        assert token == "tok_123"
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert args[0] == "https://uk-shop.myshopify.com/admin/oauth/access_token"
        assert "json" not in kwargs
        assert kwargs["data"] == {
            "grant_type": "client_credentials",
            "client_id": "cid",
            "client_secret": SECRET,
        }
        assert kwargs["timeout"] == 30

    @patch("api.lib.shopify_api.requests.post")
    def test_custom_timeout_is_forwarded(self, mock_post):
        mock_post.return_value = _resp(200, {"access_token": "t"})
        shopify_api.fetch_access_token("s.myshopify.com", "cid", SECRET, timeout=5)
        assert mock_post.call_args[1]["timeout"] == 5

    @pytest.mark.parametrize("status", [400, 401, 403, 429, 500])
    @patch("api.lib.shopify_api.requests.post")
    def test_non_200_raises_without_leaking_secret(self, mock_post, status):
        # Body and text echo the secret: the error must not forward either.
        mock_post.return_value = _resp(
            status, {"error": f"bad secret {SECRET}"}, text=f"bad secret {SECRET}"
        )

        with pytest.raises(RuntimeError) as exc:
            shopify_api.fetch_access_token("uk-shop.myshopify.com", "cid", SECRET)

        msg = str(exc.value)
        assert str(status) in msg
        assert "uk-shop.myshopify.com" in msg
        assert SECRET not in msg

    @pytest.mark.parametrize("body", [{}, {"access_token": ""}, {"access_token": None}])
    @patch("api.lib.shopify_api.requests.post")
    def test_missing_token_raises_without_leaking_secret(self, mock_post, body):
        body = dict(body, echo=SECRET)
        mock_post.return_value = _resp(200, body)

        with pytest.raises(RuntimeError) as exc:
            shopify_api.fetch_access_token("uk-shop.myshopify.com", "cid", SECRET)

        assert "no access_token" in str(exc.value)
        assert SECRET not in str(exc.value)


    # -- domain validation: must raise BEFORE any HTTP call ----------------

    @pytest.mark.parametrize("domain", [None, "", "   ", "https://", "/"])
    @patch("api.lib.shopify_api.requests.post")
    def test_missing_domain_raises_before_http(self, mock_post, domain):
        with pytest.raises(RuntimeError) as exc:
            shopify_api.fetch_access_token(domain, "cid", SECRET)

        mock_post.assert_not_called()
        assert SECRET not in str(exc.value)

    @pytest.mark.parametrize("domain", [
        "evil.example.com",                     # not myshopify
        "myshopify.com",                        # no shop label
        "foo.myshopify.com.evil.com",           # suffix trick
        "evilmyshopify.com",
        "foo.shopify.com",
        "localhost",
        "169.254.169.254",                      # metadata IP
        "foo.myshopify.com:8443",               # port
        "user@foo.myshopify.com",               # userinfo
        "foo.myshopify.com@evil.com",
        "foo bar.myshopify.com",
        "-foo.myshopify.com",                   # leading hyphen
    ])
    @patch("api.lib.shopify_api.requests.post")
    def test_non_myshopify_domain_raises_before_http(self, mock_post, domain):
        with pytest.raises(RuntimeError) as exc:
            shopify_api.fetch_access_token(domain, "cid", SECRET)

        mock_post.assert_not_called()
        assert "myshopify.com" in str(exc.value)
        assert SECRET not in str(exc.value)

    @pytest.mark.parametrize("domain", [
        "foo.myshopify.com/admin",
        "https://foo.myshopify.com/admin/api",
        "foo.myshopify.com/../evil.com",
        "foo.myshopify.com?x=1",
        "foo.myshopify.com#frag",
    ])
    @patch("api.lib.shopify_api.requests.post")
    def test_domain_with_path_or_query_raises_before_http(self, mock_post, domain):
        with pytest.raises(RuntimeError) as exc:
            shopify_api.fetch_access_token(domain, "cid", SECRET)

        mock_post.assert_not_called()
        assert SECRET not in str(exc.value)

    # -- normalisation ------------------------------------------------------

    @pytest.mark.parametrize("raw", [
        "https://Foo.MyShopify.com/",
        "HTTP://FOO.MYSHOPIFY.COM",
        "  foo.myshopify.com/  ",
    ])
    @patch("api.lib.shopify_api.requests.post")
    def test_domain_is_normalised_before_building_url(self, mock_post, raw):
        mock_post.return_value = _resp(200, {"access_token": "tok"})

        assert shopify_api.fetch_access_token(raw, "cid", SECRET) == "tok"

        assert mock_post.call_args[0][0] == "https://foo.myshopify.com/admin/oauth/access_token"

    # -- wire format --------------------------------------------------------

    def test_body_is_form_urlencoded_on_the_wire(self):
        """Prepare a real request (no socket) and inspect headers + body."""
        sent = []

        def fake_send(session, prepared, **kwargs):
            sent.append(prepared)
            return _resp(200, {"access_token": "tok"})

        with patch.object(requests.Session, "send", autospec=True, side_effect=fake_send):
            shopify_api.fetch_access_token("uk-shop.myshopify.com", "my id", "s&c=ret")

        assert len(sent) == 1
        prepared = sent[0]
        assert prepared.method == "POST"
        assert prepared.url == "https://uk-shop.myshopify.com/admin/oauth/access_token"
        assert prepared.headers["Content-Type"] == "application/x-www-form-urlencoded"
        # Values with reserved characters must be percent-encoded, not raw JSON.
        assert prepared.body == (
            "grant_type=client_credentials&client_id=my+id&client_secret=s%26c%3Dret"
        )

    # -- token masking / logging -------------------------------------------

    @patch("api.lib.shopify_api.requests.post")
    def test_github_actions_masks_token_in_a_single_line(self, mock_post, monkeypatch, capsys):
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        mock_post.return_value = _resp(200, {"access_token": "shpat_TOKEN_VALUE"})

        shopify_api.fetch_access_token("uk-shop.myshopify.com", "cid", SECRET)

        out, err = capsys.readouterr()
        assert out == "::add-mask::shpat_TOKEN_VALUE\n"
        assert "shpat_TOKEN_VALUE" not in err
        assert SECRET not in out + err

    @pytest.mark.parametrize("flag", [None, "false", "", "1"])
    @patch("api.lib.shopify_api.requests.post")
    def test_token_is_never_printed_outside_github_actions(self, mock_post, monkeypatch, capsys, flag):
        if flag is None:
            monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
        else:
            monkeypatch.setenv("GITHUB_ACTIONS", flag)
        mock_post.return_value = _resp(200, {"access_token": "shpat_TOKEN_VALUE"})

        shopify_api.fetch_access_token("uk-shop.myshopify.com", "cid", SECRET)

        out, err = capsys.readouterr()
        assert "shpat_TOKEN_VALUE" not in out + err
        assert SECRET not in out + err

    @pytest.mark.parametrize("status", [401, 429])
    @patch("api.lib.shopify_api.requests.post")
    def test_failed_exchange_prints_no_mask_line(self, mock_post, monkeypatch, capsys, status):
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        mock_post.return_value = _resp(status, {"access_token": "should_not_matter"})

        with pytest.raises(RuntimeError):
            shopify_api.fetch_access_token("uk-shop.myshopify.com", "cid", SECRET)

        out, err = capsys.readouterr()
        assert "should_not_matter" not in out + err


class TestEnsureAccessToken:

    @patch("api.lib.shopify_api.fetch_access_token", return_value="runtime_tok")
    def test_client_credentials_override_existing_token(self, mock_fetch, monkeypatch):
        monkeypatch.setenv("SHOPIFY_ACCESS_TOKEN", "existing")
        monkeypatch.setenv("SHOPIFY_CLIENT_ID", "cid")
        monkeypatch.setenv("SHOPIFY_CLIENT_SECRET", SECRET)
        monkeypatch.setenv("SHOPIFY_STORE_DOMAIN", "uk.myshopify.com")

        assert shopify_api.ensure_access_token() is True

        mock_fetch.assert_called_once_with("uk.myshopify.com", "cid", SECRET)
        assert os.environ["SHOPIFY_ACCESS_TOKEN"] == "runtime_tok"

    @pytest.mark.parametrize("present", ["SHOPIFY_CLIENT_ID", "SHOPIFY_CLIENT_SECRET", None])
    @patch("api.lib.shopify_api.fetch_access_token")
    def test_noop_when_client_credentials_incomplete(self, mock_fetch, monkeypatch, present):
        monkeypatch.setenv("SHOPIFY_STORE_DOMAIN", "uk.myshopify.com")
        if present:
            monkeypatch.setenv(present, "x")

        assert shopify_api.ensure_access_token() is False

        mock_fetch.assert_not_called()
        assert "SHOPIFY_ACCESS_TOKEN" not in os.environ

    @pytest.mark.parametrize("present", ["SHOPIFY_CLIENT_ID", "SHOPIFY_CLIENT_SECRET", None])
    @patch("api.lib.shopify_api.fetch_access_token")
    def test_existing_token_kept_when_client_credentials_incomplete(
            self, mock_fetch, monkeypatch, present):
        monkeypatch.setenv("SHOPIFY_ACCESS_TOKEN", "static_tok")
        monkeypatch.setenv("SHOPIFY_STORE_DOMAIN", "uk.myshopify.com")
        if present:
            monkeypatch.setenv(present, "x")

        assert shopify_api.ensure_access_token() is False

        mock_fetch.assert_not_called()
        assert os.environ["SHOPIFY_ACCESS_TOKEN"] == "static_tok"

    @patch("api.lib.shopify_api.fetch_access_token", return_value="runtime_tok")
    def test_sets_env_when_exchange_needed(self, mock_fetch, monkeypatch):
        monkeypatch.setenv("SHOPIFY_CLIENT_ID", "cid")
        monkeypatch.setenv("SHOPIFY_CLIENT_SECRET", SECRET)
        monkeypatch.setenv("SHOPIFY_STORE_DOMAIN", "uk.myshopify.com")

        assert shopify_api.ensure_access_token() is True

        mock_fetch.assert_called_once_with("uk.myshopify.com", "cid", SECRET)
        assert os.environ["SHOPIFY_ACCESS_TOKEN"] == "runtime_tok"

    @patch("api.lib.shopify_api.fetch_access_token",
           side_effect=RuntimeError("exchange failed"))
    def test_exchange_failure_propagates_and_sets_nothing(self, mock_fetch, monkeypatch):
        monkeypatch.setenv("SHOPIFY_CLIENT_ID", "cid")
        monkeypatch.setenv("SHOPIFY_CLIENT_SECRET", SECRET)
        monkeypatch.setenv("SHOPIFY_STORE_DOMAIN", "uk.myshopify.com")

        with pytest.raises(RuntimeError):
            shopify_api.ensure_access_token()

        assert "SHOPIFY_ACCESS_TOKEN" not in os.environ


# ---------------------------------------------------------------------------
# 2. utils.get_current_shop_domain / get_default_currency
# ---------------------------------------------------------------------------

class TestGetCurrentShopDomain:

    def test_none_when_unset(self):
        assert get_current_shop_domain() is None

    def test_none_when_empty(self, monkeypatch):
        monkeypatch.setenv("SHOPIFY_STORE_DOMAIN", "")
        assert get_current_shop_domain() is None

    @pytest.mark.parametrize("raw, expected", [
        ("adam-lippes-uk.myshopify.com", "adam-lippes-uk.myshopify.com"),
        ("Adam-Lippes-UK.MyShopify.com", "adam-lippes-uk.myshopify.com"),
        ("https://adam-lippes-uk.myshopify.com", "adam-lippes-uk.myshopify.com"),
        ("HTTPS://Adam-Lippes-UK.myshopify.com/", "adam-lippes-uk.myshopify.com"),
        ("http://adam-lippes-uk.myshopify.com/", "adam-lippes-uk.myshopify.com"),
        ("adam-lippes-uk.myshopify.com/", "adam-lippes-uk.myshopify.com"),
        ("  adam-lippes-uk.myshopify.com  ", "adam-lippes-uk.myshopify.com"),
    ])
    def test_normalisation(self, monkeypatch, raw, expected):
        monkeypatch.setenv("SHOPIFY_STORE_DOMAIN", raw)
        assert get_current_shop_domain() == expected


class TestGetDefaultCurrency:

    @pytest.mark.parametrize("org, expected", [
        ("US", "USD"), ("JP", "JPY"), ("UK", "GBP"),
        ("uk", "GBP"),            # case-insensitive
        ("FR", "USD"),            # unknown -> USD
    ])
    def test_by_commercial_organisation(self, monkeypatch, org, expected):
        monkeypatch.setenv("COMMERCIAL_ORGANISATION", org)
        assert get_default_currency() == expected

    def test_defaults_to_usd_when_org_unset(self):
        assert get_default_currency() == "USD"

    def test_shop_currency_override_wins_and_is_uppercased(self, monkeypatch):
        monkeypatch.setenv("COMMERCIAL_ORGANISATION", "UK")
        monkeypatch.setenv("SHOP_CURRENCY", "eur")
        assert get_default_currency() == "EUR"

    def test_empty_shop_currency_falls_back_to_org(self, monkeypatch):
        monkeypatch.setenv("COMMERCIAL_ORGANISATION", "JP")
        monkeypatch.setenv("SHOP_CURRENCY", "")
        assert get_default_currency() == "JPY"


# ---------------------------------------------------------------------------
# 3. insert_order.extract_market_from_tags
# ---------------------------------------------------------------------------

class TestExtractMarketFromTags:

    @pytest.mark.parametrize("tags, expected", [
        ("UK", "UK"),
        ("uk", "UK"),
        (" Uk , wholesale", "UK"),
        ("wholesale, JP", "JP"),
        ("US", "US"),
        ('["STORE_Office_14378139719", "UK"]', "UK"),
    ])
    def test_market_tag_is_recognised(self, monkeypatch, tags, expected):
        # Org deliberately different so the tag, not the default, must win.
        monkeypatch.setenv("COMMERCIAL_ORGANISATION", "JP" if expected != "JP" else "US")
        assert extract_market_from_tags(tags) == expected

    @pytest.mark.parametrize("org", ["UK", "JP", "US"])
    def test_no_market_tag_defaults_to_current_organisation(self, monkeypatch, org):
        monkeypatch.setenv("COMMERCIAL_ORGANISATION", org)
        assert extract_market_from_tags("wholesale, STORE_Office_14378139719") == org

    def test_no_market_tag_defaults_to_us_when_org_unset(self):
        assert extract_market_from_tags("wholesale") == "US"

    def test_uk_store_orders_without_market_tag_are_not_labelled_us(self, monkeypatch):
        """Regression: the old code hard-coded 'US' as the fallback."""
        monkeypatch.setenv("COMMERCIAL_ORGANISATION", "UK")
        assert extract_market_from_tags("some-other-tag") != "US"

    @pytest.mark.parametrize("tags", [None, "", "   ", "[]", " , "])
    def test_empty_tags_return_none_whatever_the_org(self, monkeypatch, tags):
        monkeypatch.setenv("COMMERCIAL_ORGANISATION", "UK")
        assert extract_market_from_tags(tags) is None


# ---------------------------------------------------------------------------
# 4. Queue filtering by shop
# ---------------------------------------------------------------------------

SHOP_UK = "adam-lippes-uk.myshopify.com"
SHOP_JP = "adam-lippes-jp.myshopify.com"


class TestInventoryQueueShopFilter:

    def _run(self, monkeypatch, raw_domain):
        monkeypatch.setenv("SHOPIFY_STORE_DOMAIN", raw_domain)
        conn, cur = _mock_conn()
        cur.fetchall.side_effect = [[], []]  # no pending, no enrichment
        with patch("api.lib.process_inventory_sync._pg_connect", return_value=conn), \
             patch("api.lib.process_inventory_sync.get_store_context", return_value={
                 "data_source": "Shopify", "company_code": "AL",
                 "commercial_organisation": "UK"}):
            process_inventory_queue()
        return _executed(cur)

    def _queue_selects(self, executed):
        return [(sql, p) for sql, p in executed
                if sql.startswith("SELECT") and "FROM inventory_snapshot_queue" in sql]

    def test_phase_a_and_phase_b_selects_are_filtered_by_shop(self, monkeypatch):
        selects = self._queue_selects(self._run(monkeypatch, SHOP_UK))

        assert len(selects) == 2
        phase_a = [s for s in selects if "DISTINCT ON" not in s[0]]
        phase_b = [s for s in selects if "DISTINCT ON" in s[0]]
        assert len(phase_a) == 1 and len(phase_b) == 1

        for sql, params in (phase_a[0], phase_b[0]):
            assert "WHERE shop = %s" in sql
            assert params == (SHOP_UK,)
            assert sql.count("%s") == len(params)

    def test_shop_filter_does_not_weaken_status_conditions(self, monkeypatch):
        selects = self._queue_selects(self._run(monkeypatch, SHOP_UK))
        phase_a = next(s for s in selects if "DISTINCT ON" not in s[0])[0]
        phase_b = next(s for s in selects if "DISTINCT ON" in s[0])[0]

        # The OR in Phase A must be parenthesised, otherwise `shop = X AND
        # pending OR failed` would let failed rows of ANY shop through.
        assert ("shop = %s AND (status = 'pending' OR (status = 'failed' AND attempts < 6))"
                in phase_a)
        assert "status = 'completed'" in phase_b
        assert "history_synced" in phase_b

    @pytest.mark.parametrize("raw, expected", [
        (SHOP_JP, SHOP_JP),
        ("HTTPS://Adam-Lippes-JP.myshopify.com/", SHOP_JP),
    ])
    def test_param_is_the_normalised_current_domain(self, monkeypatch, raw, expected):
        selects = self._queue_selects(self._run(monkeypatch, raw))
        assert [p for _, p in selects] == [(expected,), (expected,)]

    def test_two_stores_never_share_a_filter_value(self, monkeypatch):
        uk = {p for _, p in self._queue_selects(self._run(monkeypatch, SHOP_UK))}
        jp = {p for _, p in self._queue_selects(self._run(monkeypatch, SHOP_JP))}
        assert uk == {(SHOP_UK,)}
        assert jp == {(SHOP_JP,)}
        assert uk.isdisjoint(jp)

    def test_unset_domain_skips_queue_without_touching_db(self, monkeypatch):
        """No domain -> queue skipped (no `shop = NULL` query, no DB connection)."""
        monkeypatch.delenv("SHOPIFY_STORE_DOMAIN", raising=False)
        with patch("api.lib.process_inventory_sync._pg_connect") as mock_connect:
            stats = process_inventory_queue()
        mock_connect.assert_not_called()
        assert stats["total_pending"] == 0


class TestDraftOrdersDeleteQueueShopFilter:

    def _run(self, monkeypatch, raw_domain, rows):
        monkeypatch.setenv("SHOPIFY_STORE_DOMAIN", raw_domain)
        conn, cur = _mock_conn()
        cur.fetchall.return_value = rows
        cur.rowcount = 1
        with patch("api.lib.process_draft_orders._pg_connect", return_value=conn):
            stats = process_draft_orders_delete_queue()
        return stats, _executed(cur)

    def test_select_filtered_by_shop(self, monkeypatch):
        stats, executed = self._run(monkeypatch, SHOP_UK, [])

        sql, params = executed[0]
        assert "FROM draft_orders_delete_queue" in sql
        assert "WHERE shop = %s" in sql
        assert params == (SHOP_UK,)
        assert stats["total_pending"] == 0

    @pytest.mark.parametrize("raw", [None, "", "   ", "https://", "/"])
    def test_no_domain_skips_without_touching_db(self, monkeypatch, raw):
        """No domain -> no `shop = NULL` query, no connection, nothing deleted."""
        if raw is None:
            monkeypatch.delenv("SHOPIFY_STORE_DOMAIN", raising=False)
        else:
            monkeypatch.setenv("SHOPIFY_STORE_DOMAIN", raw)

        with patch("api.lib.process_draft_orders._pg_connect") as mock_connect:
            stats = process_draft_orders_delete_queue()

        mock_connect.assert_not_called()
        assert stats == {"deleted": 0, "failed": 0, "total_pending": 0, "errors": []}

    def test_domain_is_normalised(self, monkeypatch):
        _, executed = self._run(monkeypatch, "HTTPS://Adam-Lippes-UK.myshopify.com/", [])
        assert executed[0][1] == (SHOP_UK,)

    def test_status_or_is_parenthesised_under_shop_filter(self, monkeypatch):
        _, executed = self._run(monkeypatch, SHOP_UK, [])
        sql = executed[0][0]
        assert "shop = %s AND (status = 'pending' OR (status = 'failed' AND attempts < 6))" in sql

    def test_two_stores_use_different_filter_values(self, monkeypatch):
        _, uk = self._run(monkeypatch, SHOP_UK, [])
        _, jp = self._run(monkeypatch, SHOP_JP, [])
        assert uk[0][1] == (SHOP_UK,)
        assert jp[0][1] == (SHOP_JP,)

    def test_rows_returned_by_filtered_select_are_processed(self, monkeypatch):
        stats, executed = self._run(monkeypatch, SHOP_UK, [(7, 999)])

        assert stats["deleted"] == 1
        assert stats["failed"] == 0
        # Only the first statement is the queue SELECT; the others are keyed by id.
        selects = [e for e in executed if e[0].startswith("SELECT")]
        assert len(selects) == 1
        assert any("UPDATE draft_order SET status = 'deleted'" in sql and p == (999,)
                   for sql, p in executed)


# ---------------------------------------------------------------------------
# 5. Watermarks filtered by commercial_organisation
# ---------------------------------------------------------------------------

class TestWatermarksAreScopedToOrganisation:

    def _product_cur(self, latest):
        conn, cur = _mock_conn()
        cur.fetchone.side_effect = [(True,), (latest,)]
        return conn, cur

    @pytest.mark.parametrize("org", ["US", "JP", "UK"])
    def test_latest_product_update_date_passes_org(self, monkeypatch, org):
        monkeypatch.setenv("COMMERCIAL_ORGANISATION", org)
        conn, cur = self._product_cur(datetime(2026, 3, 1, 12, 0, 0))

        with patch("api.lib.product_processor._pg_connect", return_value=conn):
            result = get_latest_product_update_date()

        assert result == "2026-03-01T12:00:00"
        watermark = [(s, p) for s, p in _executed(cur) if "FROM products" in s]
        assert len(watermark) == 1
        sql, params = watermark[0]
        assert "commercial_organisation = %s" in sql
        assert params == (org,)

    @pytest.mark.parametrize("org", ["US", "JP", "UK"])
    def test_latest_location_date_passes_org(self, monkeypatch, org):
        monkeypatch.setenv("COMMERCIAL_ORGANISATION", org)
        conn, cur = _mock_conn()
        cur.fetchone.side_effect = [(True,), (datetime(2026, 2, 1, 8, 30, 0),)]

        with patch("api.lib.location_processor._pg_connect", return_value=conn):
            result = get_latest_location_date()

        assert result == "2026-02-01T08:30:00"
        watermark = [(s, p) for s, p in _executed(cur) if "FROM locations" in s]
        assert len(watermark) == 1
        sql, params = watermark[0]
        assert "commercial_organisation = %s" in sql
        assert params == (org,)

    def test_org_defaults_to_us_when_unset(self):
        conn, cur = self._product_cur(datetime(2026, 3, 1))
        with patch("api.lib.product_processor._pg_connect", return_value=conn):
            get_latest_product_update_date()
        sql, params = next((s, p) for s, p in _executed(cur) if "FROM products" in s)
        assert params == ("US",)

    def test_product_watermark_none_when_org_has_no_rows(self, monkeypatch):
        """A new store (UK) with no rows yet must get a full sync, not another store's date."""
        monkeypatch.setenv("COMMERCIAL_ORGANISATION", "UK")
        conn, cur = self._product_cur(datetime(1970, 1, 1))
        with patch("api.lib.product_processor._pg_connect", return_value=conn):
            assert get_latest_product_update_date() is None

    def test_location_watermark_none_when_org_has_no_rows(self, monkeypatch):
        monkeypatch.setenv("COMMERCIAL_ORGANISATION", "UK")
        conn, cur = _mock_conn()
        cur.fetchone.side_effect = [(True,), (None,)]
        with patch("api.lib.location_processor._pg_connect", return_value=conn):
            assert get_latest_location_date() is None


# ---------------------------------------------------------------------------
# 6. Currency fallback (draft orders)
# ---------------------------------------------------------------------------

def _draft(**overrides):
    d = {
        "id": 5551,
        "status": "open",
        "created_at": "2026-04-01T10:00:00Z",
        "completed_at": None,
        "order_id": None,
        "tags": "",
        "customer": {"id": 42},
        "name": "#D1",
        "note": None,
        "line_items": [{
            "product_id": 1, "title": "Dress", "price": "100.00", "quantity": 2,
            "sku": "S1", "variant_id": 3, "variant_title": "M", "name": "Dress - M",
            "tax_lines": [{"title": "VAT", "price": "20.00"}],
        }],
        "shipping_line": {"price": "10.00"},
    }
    d.update(overrides)
    return d


class TestDraftOrderCurrencyFallback:

    @pytest.mark.parametrize("org, expected", [("UK", "GBP"), ("JP", "JPY"), ("US", "USD")])
    def test_missing_currency_uses_store_default(self, monkeypatch, org, expected):
        monkeypatch.setenv("COMMERCIAL_ORGANISATION", org)

        txs = process_draft_order(_draft())

        # item + tax + shipping
        assert {t["type"] for t in txs} == {
            "draft_order_item", "draft_order_tax", "draft_order_shipping"}
        assert {t["transaction_currency"] for t in txs} == {expected}

    def test_shop_currency_override_applies_to_fallback(self, monkeypatch):
        monkeypatch.setenv("COMMERCIAL_ORGANISATION", "UK")
        monkeypatch.setenv("SHOP_CURRENCY", "eur")

        txs = process_draft_order(_draft())

        assert {t["transaction_currency"] for t in txs} == {"EUR"}

    def test_currency_from_shopify_is_not_overridden(self, monkeypatch):
        monkeypatch.setenv("COMMERCIAL_ORGANISATION", "UK")

        txs = process_draft_order(_draft(currency="EUR"))

        assert {t["transaction_currency"] for t in txs} == {"EUR"}

    def test_uk_draft_without_currency_is_not_labelled_usd(self, monkeypatch):
        """Regression: the old code hard-coded 'USD'."""
        monkeypatch.setenv("COMMERCIAL_ORGANISATION", "UK")
        txs = process_draft_order(_draft())
        assert all(t["transaction_currency"] != "USD" for t in txs)


# ---------------------------------------------------------------------------
# 7. Refund location fallback (process_transactions.get_refund_details)
# ---------------------------------------------------------------------------
# The fallback is inline in get_refund_details, so that is the smallest unit
# that exposes it. Everything it touches (HTTP, DB lookups) is patched.

BERGEN_WAREHOUSE = 31738513
REFUND_LOCATION = 777001
SALE_LOCATION = 888002


def _refund_payload(location_id, **line_overrides):
    money = lambda amt: {  # noqa: E731
        "shop_money": {"amount": amt, "currency_code": "GBP"},
        "presentment_money": {"amount": amt, "currency_code": "GBP"},
    }
    line_item = {
        "product_id": 11, "variant_id": 22, "name": "Dress - M",
        "price_set": money("100.00"),
        "tax_lines": [{"title": "VAT", "price_set": money("20.00")}],
        "duties": [{"price_set": money("5.00")}],
        "discount_allocations": [{"amount_set": money("10.00"),
                                  "discount_application_index": 0}],
    }
    line_item.update(line_overrides)
    return {"refund": {
        "created_at": "2026-04-01T10:00:00Z",
        "location_id": location_id,
        "refund_line_items": [{"restock_type": "return", "quantity": 1,
                               "line_item": line_item}],
        "order_adjustments": [],
    }}


def _run_refund(payload, *, source_name="web", sale_location_ids=None,
                return_check=False, order_cancelled_at=None):
    with patch("api.lib.process_transactions.requests.get",
               return_value=_resp(200, payload)), \
         patch("api.lib.process_transactions._shopify_headers", return_value={}), \
         patch("api.lib.process_transactions.check_return_check",
               return_value=return_check), \
         patch("api.lib.process_transactions.get_orders_details_id", return_value=1), \
         patch("api.lib.process_transactions.calculate_cogs_values",
               return_value=(1.0, 1.0)):
        return get_refund_details(
            "order-1", "refund-1", "client-1", source_name, "card",
            sale_location_ids=sale_location_ids,
            order_cancelled_at=order_cancelled_at,
        )


class TestRefundLocationFallback:

    @pytest.mark.parametrize("org", ["US", "us", None])
    @pytest.mark.parametrize("refund_location", [REFUND_LOCATION, None])
    def test_us_keeps_bergen_warehouse_fallback(self, monkeypatch, org, refund_location):
        """US behaviour is unchanged, whatever location the refund carries.

        org=None covers an unset COMMERCIAL_ORGANISATION, which defaults to US.
        """
        if org:
            monkeypatch.setenv("COMMERCIAL_ORGANISATION", org)

        items = _run_refund(_refund_payload(refund_location))

        assert {i["location_id"] for i in items} == {BERGEN_WAREHOUSE}

    @pytest.mark.parametrize("org", ["JP", "UK", "uk"])
    def test_non_us_falls_back_to_the_refunds_own_location(self, monkeypatch, org):
        monkeypatch.setenv("COMMERCIAL_ORGANISATION", org)

        items = _run_refund(_refund_payload(REFUND_LOCATION))

        # refund_line_item + tax_line + duties_charge + discount_allocation
        assert {i["type"] for i in items} == {
            "refund_line_item", "tax_line", "duties_charge", "discount_allocation"}
        assert {i["location_id"] for i in items} == {REFUND_LOCATION}

    @pytest.mark.parametrize("org", ["JP", "UK"])
    def test_non_us_without_refund_location_is_none_never_bergen(self, monkeypatch, org):
        monkeypatch.setenv("COMMERCIAL_ORGANISATION", org)

        items = _run_refund(_refund_payload(None))

        assert items, "expected refund items"
        assert all(i["location_id"] is None for i in items)
        assert BERGEN_WAREHOUSE not in {i["location_id"] for i in items}

    @pytest.mark.parametrize("org", ["JP", "UK"])
    def test_non_us_order_adjustment_row_never_gets_bergen(self, monkeypatch, org):
        monkeypatch.setenv("COMMERCIAL_ORGANISATION", org)
        payload = {"refund": {
            "created_at": "2026-04-01T10:00:00Z",
            "location_id": REFUND_LOCATION,
            "refund_line_items": [],
            "order_adjustments": [{"amount_set": {
                "shop_money": {"amount": "-3.00", "currency_code": "GBP"},
                "presentment_money": {"amount": "-3.00", "currency_code": "GBP"}}}],
        }}

        items = _run_refund(payload)

        assert [i["type"] for i in items] == ["refund_discrepancy"]
        assert items[0]["location_id"] == REFUND_LOCATION

    @pytest.mark.parametrize("org", ["US", "UK"])
    def test_sale_location_still_wins_when_pos_return_conditions_hold(self, monkeypatch, org):
        monkeypatch.setenv("COMMERCIAL_ORGANISATION", org)

        items = _run_refund(
            _refund_payload(REFUND_LOCATION),
            source_name="pos",
            sale_location_ids={(11, 22): SALE_LOCATION},
            return_check=True,
        )

        assert {i["location_id"] for i in items} == {SALE_LOCATION}

    def test_uk_pos_return_without_matching_sale_uses_refund_location(self, monkeypatch):
        """Conditions hold but the sale location is unknown -> fallback, not Bergen."""
        monkeypatch.setenv("COMMERCIAL_ORGANISATION", "UK")

        items = _run_refund(
            _refund_payload(REFUND_LOCATION),
            source_name="pos",
            sale_location_ids={(99, 99): SALE_LOCATION},   # different product/variant
            return_check=True,
        )

        assert {i["location_id"] for i in items} == {REFUND_LOCATION}

    def test_uk_sale_location_ignored_when_conditions_do_not_hold(self, monkeypatch):
        monkeypatch.setenv("COMMERCIAL_ORGANISATION", "UK")

        items = _run_refund(
            _refund_payload(REFUND_LOCATION),
            source_name="web",                              # not pos
            sale_location_ids={(11, 22): SALE_LOCATION},
            return_check=True,
        )

        assert {i["location_id"] for i in items} == {REFUND_LOCATION}

    def test_refund_http_error_returns_empty_list(self, monkeypatch):
        monkeypatch.setenv("COMMERCIAL_ORGANISATION", "UK")
        with patch("api.lib.process_transactions.requests.get",
                   return_value=_resp(429, text="slow down")), \
             patch("api.lib.process_transactions._shopify_headers", return_value={}):
            assert get_refund_details("o", "r", "c", "web", None) == []


# ---------------------------------------------------------------------------
# 8. Entrypoint import order (regression guard, runs in a subprocess)
# ---------------------------------------------------------------------------
# process_customer, process_payout and process_inventory_sync read the Shopify
# token/domain at import time. run_daily_sync must therefore call
# ensure_access_token() BEFORE any of them is imported. A subprocess keeps this
# independent of what other tests already put in sys.modules.

ENV_READING_MODULES = (
    "api.lib.process_customer",
    "api.lib.process_payout",
    "api.lib.process_inventory_sync",
)


def _clean_subprocess_env():
    """Minimal env: no Shopify/DB vars, whatever the dev shell or CI exports."""
    env = {"PATH": os.environ.get("PATH", ""), "HOME": os.environ.get("HOME", "")}
    for name in ("SYSTEMROOT", "TMPDIR", "LANG", "LC_ALL"):
        if name in os.environ:
            env[name] = os.environ[name]
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


# The repo's .env must not leak into the child: neutralise load_dotenv before
# the code under test imports it.
_NO_DOTENV = (
    "import dotenv; dotenv.load_dotenv = lambda *a, **k: False; "
    "dotenv.find_dotenv = lambda *a, **k: ''\n"
)

_IMPORT_ORDER_SCRIPT = _NO_DOTENV + textwrap.dedent("""
    import json, sys
    FORBIDDEN = %(forbidden)r

    def loaded():
        return sorted(m for m in FORBIDDEN if m in sys.modules)

    import run_daily_sync
    report = {
        "after_import": loaded(),
        "shopify_api_loaded": "api.lib.shopify_api" in sys.modules,
    }

    # Entering main(): stop at ensure_access_token(), before any processor
    # import and before anything can reach Shopify or the DB.
    def fake_ensure():
        report["at_ensure_access_token"] = loaded()
        raise RuntimeError("stop-before-processors")

    run_daily_sync.ensure_access_token = fake_ensure
    try:
        run_daily_sync.main()
    except SystemExit as exc:
        report["exit_code"] = exc.code
    report["after_main"] = loaded()
    sys.stdout.write("REPORT=" + json.dumps(report) + "\\n")
""") % {"forbidden": ENV_READING_MODULES}


@pytest.fixture(scope="module")
def import_order_report():
    proc = subprocess.run(
        [sys.executable, "-c", _IMPORT_ORDER_SCRIPT],
        cwd=REPO_ROOT, env=_clean_subprocess_env(),
        capture_output=True, text=True, timeout=60,
    )
    lines = [ln for ln in proc.stdout.splitlines() if ln.startswith("REPORT=")]
    assert lines, f"no report; rc={proc.returncode}\nstdout={proc.stdout}\nstderr={proc.stderr}"
    return json.loads(lines[-1][len("REPORT="):])


class TestEntrypointImportOrder:

    def test_harness_is_not_vacuous(self, import_order_report):
        assert import_order_report["shopify_api_loaded"] is True
        assert import_order_report["exit_code"] == 1   # fake_ensure aborted main()

    def test_importing_run_daily_sync_does_not_import_env_reading_modules(
            self, import_order_report):
        assert import_order_report["after_import"] == []

    def test_main_has_not_imported_them_when_token_exchange_runs(self, import_order_report):
        assert import_order_report["at_ensure_access_token"] == []
        assert import_order_report["after_main"] == []


# ---------------------------------------------------------------------------
# 9. --org validation in the backfill / status scripts (subprocess)
# ---------------------------------------------------------------------------
# Both scripts parse --org at import time and must refuse anything outside
# US/JP/UK before touching the env, the filesystem or the network. The child
# runs with no .env, no SHOPIFY_*/DB vars, and every socket connect turned
# into a hard failure (exit 99) so "no network" is enforced, not assumed.

_ORG_SCRIPT = _NO_DOTENV + textwrap.dedent("""
    import runpy, socket, sys

    def _no_network(*a, **k):
        sys.stderr.write("NETWORK-ATTEMPT\\n")
        raise SystemExit(99)

    socket.socket.connect = _no_network
    socket.create_connection = _no_network
    socket.getaddrinfo = _no_network

    script = sys.argv[1]
    sys.argv = [script] + sys.argv[2:]
    runpy.run_path(script, run_name="__main__")
""")

ORG_SCRIPTS = ["backfill_store_data.py", "check_store_backfill_status.py"]


def _run_org_script(script, *args):
    return subprocess.run(
        [sys.executable, "-c", _ORG_SCRIPT, os.path.join(REPO_ROOT, script), *args],
        cwd=REPO_ROOT, env=_clean_subprocess_env(),
        capture_output=True, text=True, timeout=60,
    )


@pytest.mark.parametrize("script", ORG_SCRIPTS)
class TestOrgArgumentValidation:

    @pytest.mark.parametrize("args", [
        ["--org", "../X"],
        ["--org", "FR"],
        ["--org", "../../etc"],
        ["--org", "UK/../US"],
        ["--org", "U K"],
        ["--org", ""],
        ["--org", "UK;rm"],
        ["--org", "UKK"],
    ])
    def test_invalid_org_is_rejected_cleanly(self, script, args):
        proc = _run_org_script(script, *args)
        out = proc.stdout + proc.stderr

        assert proc.returncode == 1
        assert "--org invalide" in out
        assert "JP, UK, US" in out                      # allowed list is shown
        assert "Traceback" not in out
        # Failure is the org validation, not a later missing-config check.
        assert "SHOPIFY_STORE_DOMAIN" not in out
        assert "NETWORK-ATTEMPT" not in out

    def test_missing_org_is_rejected_cleanly(self, script):
        proc = _run_org_script(script)
        out = proc.stdout + proc.stderr

        assert proc.returncode == 1
        assert "--org est requis" in out
        assert "Traceback" not in out
        assert "SHOPIFY_STORE_DOMAIN" not in out
        assert "NETWORK-ATTEMPT" not in out

    def test_org_flag_without_value_is_rejected_cleanly(self, script):
        proc = _run_org_script(script, "--org")
        out = proc.stdout + proc.stderr

        assert proc.returncode == 1
        assert "--org requiert une valeur" in out
        assert "Traceback" not in out
        assert "SHOPIFY_STORE_DOMAIN" not in out

    def test_control_valid_org_gets_past_validation_and_stops_on_missing_domain(self, script):
        """Proves the harness env is really clean: a VALID org fails later, on
        the missing domain. Without this, the negative tests above could pass
        for the wrong reason."""
        proc = _run_org_script(script, "--org", "uk")   # case-insensitive
        out = proc.stdout + proc.stderr

        assert proc.returncode == 1
        assert "SHOPIFY_STORE_DOMAIN_UK" in out
        assert "--org invalide" not in out
        assert "Traceback" not in out
        assert "NETWORK-ATTEMPT" not in out


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
