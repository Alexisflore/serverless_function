#!/usr/bin/env python3
"""
Test que process_draft_order traite correctement les tags et tags_list.
Utilise des données simulées (pas besoin de Shopify ni de DB).
"""

import sys
import os
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.lib.process_draft_orders import process_draft_order, _parse_tags_to_list


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_draft(draft_id, tags="", note=None, line_items=None, shipping_price=None):
    """Fabrique un draft order minimal pour les tests."""
    items = line_items or [
        {
            "id": 111,
            "product_id": 222,
            "title": "Test Product",
            "sku": "SKU-001",
            "variant_id": 333,
            "variant_title": "S / Black",
            "name": "Test Product - S / Black",
            "quantity": 1,
            "price": "100.00",
            "tax_lines": [{"title": "State Tax", "price": "8.00"}],
        }
    ]
    draft = {
        "id": draft_id,
        "name": f"#D{draft_id}",
        "status": "completed",
        "created_at": "2025-06-01T10:00:00-04:00",
        "updated_at": "2025-06-01T10:05:00-04:00",
        "completed_at": "2025-06-01T10:05:00-04:00",
        "order_id": 900000 + draft_id,
        "currency": "USD",
        "note": note,
        "tags": tags,
        "customer": {"id": 55555},
        "line_items": items,
        "shipping_line": {"price": str(shipping_price)} if shipping_price else None,
        "tax_lines": [],
    }
    return draft


def check(label, condition):
    status = "PASS" if condition else "FAIL"
    print(f"  [{status}] {label}")
    return condition


# ---------------------------------------------------------------------------
# Tests _parse_tags_to_list
# ---------------------------------------------------------------------------

def test_parse_tags():
    print("\n=== Test _parse_tags_to_list ===")
    ok = True

    ok &= check("Empty string -> None", _parse_tags_to_list("") is None)
    ok &= check("None -> None", _parse_tags_to_list(None) is None)
    ok &= check("Whitespace -> None", _parse_tags_to_list("   ") is None)

    result = _parse_tags_to_list("US, STORE_Office_12345, VIP")
    ok &= check(
        "Comma-separated -> list of 3",
        result == ["US", "STORE_Office_12345", "VIP"],
    )

    result = _parse_tags_to_list("single_tag")
    ok &= check("Single tag -> list of 1", result == ["single_tag"])

    result = _parse_tags_to_list('["US", "JP"]')
    ok &= check("JSON array string -> list", result == ["US", "JP"])

    return ok


# ---------------------------------------------------------------------------
# Tests process_draft_order — tags propagation
# ---------------------------------------------------------------------------

def test_tags_in_transactions():
    print("\n=== Test tags propagation dans les transactions ===")
    ok = True

    tags_str = "US, STORE_Madison_72498192539, VIP"
    draft = make_draft(1001, tags=tags_str, shipping_price=15.00)
    txns = process_draft_order(draft)

    ok &= check(f"Nombre de transactions = 3 (item + tax + shipping)", len(txns) == 3)

    expected_list = ["US", "STORE_Madison_72498192539", "VIP"]

    for txn in txns:
        t = txn["type"]
        ok &= check(
            f"[{t}] tags == raw string",
            txn.get("tags") == tags_str,
        )
        ok &= check(
            f"[{t}] tags_list == {expected_list}",
            txn.get("tags_list") == expected_list,
        )
        ok &= check(
            f"[{t}] source_location == 72498192539 (from STORE_ tag)",
            txn.get("source_location") == 72498192539,
        )

    return ok


def test_empty_tags():
    print("\n=== Test tags vides ===")
    ok = True

    draft = make_draft(2001, tags="")
    txns = process_draft_order(draft)

    for txn in txns:
        t = txn["type"]
        ok &= check(f"[{t}] tags == None (empty string)", txn.get("tags") is None)
        ok &= check(f"[{t}] tags_list == None", txn.get("tags_list") is None)
        ok &= check(f"[{t}] source_location == None", txn.get("source_location") is None)

    return ok


def test_tags_without_store():
    print("\n=== Test tags sans STORE_ ===")
    ok = True

    draft = make_draft(3001, tags="US, VIP, wholesale")
    txns = process_draft_order(draft)

    expected_list = ["US", "VIP", "wholesale"]
    for txn in txns:
        t = txn["type"]
        ok &= check(f"[{t}] tags_list == {expected_list}", txn.get("tags_list") == expected_list)
        ok &= check(f"[{t}] source_location == None (no STORE_ tag)", txn.get("source_location") is None)

    return ok


def test_json_serialization():
    print("\n=== Test sérialisation JSON de tags_list (simule l'insert DB) ===")
    ok = True

    draft = make_draft(4001, tags="US, JP, test")
    txns = process_draft_order(draft)

    for txn in txns:
        tl = txn.get("tags_list")
        if isinstance(tl, list):
            serialized = json.dumps(tl)
            deserialized = json.loads(serialized)
            ok &= check(
                f"[{txn['type']}] JSON round-trip OK: {deserialized}",
                deserialized == tl,
            )
        else:
            ok &= check(f"[{txn['type']}] tags_list is None (expected list)", False)

    return ok


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 60)
    print("TEST: Vérification du traitement des tags dans draft orders")
    print("=" * 60)

    all_ok = True
    all_ok &= test_parse_tags()
    all_ok &= test_tags_in_transactions()
    all_ok &= test_empty_tags()
    all_ok &= test_tags_without_store()
    all_ok &= test_json_serialization()

    print("\n" + "=" * 60)
    if all_ok:
        print("RESULTAT: Tous les tests sont PASS")
    else:
        print("RESULTAT: Certains tests ont FAIL")
        sys.exit(1)
    print("=" * 60)
