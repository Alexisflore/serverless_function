#!/usr/bin/env python3
"""
Script one-shot : balaye tous les draft orders de la table draft_order,
appelle l'API Shopify pour vérifier leur statut actuel et met à jour
la colonne status en base si elle diffère.

- Si Shopify renvoie 404 → le draft order a été supprimé → status = 'deleted'
- Sinon on prend le status retourné par Shopify (open, completed, invoice_sent…)
- Respecte le rate-limit Shopify (pause entre chaque appel)
"""

import os
import sys
import time
import requests
import psycopg2
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN")
API_VERSION = "2024-10"

def _shopify_headers():
    return {
        "X-Shopify-Access-Token": os.getenv("SHOPIFY_ACCESS_TOKEN"),
        "Content-Type": "application/json",
    }

def _pg_connect():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        db_url = "postgresql://{user}:{pw}@{host}:{port}/{db}".format(
            user=os.getenv("SUPABASE_USER"),
            pw=os.getenv("SUPABASE_PASSWORD"),
            host=os.getenv("SUPABASE_HOST"),
            port=os.getenv("SUPABASE_PORT"),
            db=os.getenv("SUPABASE_DB_NAME"),
        )
    return psycopg2.connect(db_url)


def get_shopify_draft_order_status(draft_id: int) -> str | None:
    """
    Appelle GET /admin/api/.../draft_orders/{id}.json
    Retourne le status Shopify ou 'deleted' si 404.
    """
    url = f"https://{STORE_DOMAIN}/admin/api/{API_VERSION}/draft_orders/{draft_id}.json?fields=id,status"
    resp = requests.get(url, headers=_shopify_headers())

    if resp.status_code == 404:
        return "deleted"
    if resp.status_code == 429:
        retry_after = float(resp.headers.get("Retry-After", 2))
        print(f"    Rate limited, pause {retry_after}s...")
        time.sleep(retry_after)
        return get_shopify_draft_order_status(draft_id)
    if not resp.ok:
        print(f"    ⚠️ Erreur HTTP {resp.status_code} pour draft {draft_id}: {resp.text[:200]}")
        return None

    return resp.json().get("draft_order", {}).get("status")


def sync_all_draft_order_statuses():
    conn = _pg_connect()
    cur = conn.cursor()

    # Récupérer tous les draft_ids distincts avec leur status actuel en base
    cur.execute("""
        SELECT DISTINCT _draft_id, status
        FROM draft_order
        ORDER BY _draft_id
    """)
    rows = cur.fetchall()
    total = len(rows)
    print(f"📋 {total} draft orders distincts à vérifier sur Shopify\n")

    stats = {"unchanged": 0, "updated": 0, "errors": 0}

    for i, (draft_id, db_status) in enumerate(rows):
        if (i + 1) % 50 == 0 or i == 0:
            print(f"--- Progression: {i + 1}/{total} ---")

        shopify_status = get_shopify_draft_order_status(draft_id)

        if shopify_status is None:
            stats["errors"] += 1
            continue

        if shopify_status == db_status:
            stats["unchanged"] += 1
            continue

        # Status différent → mise à jour
        print(f"  🔄 Draft {draft_id}: {db_status} → {shopify_status}")
        cur.execute("""
            UPDATE draft_order
            SET status = %s
            WHERE _draft_id = %s
        """, (shopify_status, draft_id))
        stats["updated"] += 1

        # Commit par lots de 50 pour ne pas tout perdre en cas de crash
        if stats["updated"] % 50 == 0:
            conn.commit()

        # Petite pause pour respecter le rate-limit (2 req/s de marge)
        time.sleep(0.5)

    conn.commit()
    cur.close()
    conn.close()

    print(f"\n{'='*50}")
    print(f"📊 Résultats:")
    print(f"  - Total vérifié:  {total}")
    print(f"  - Inchangés:      {stats['unchanged']}")
    print(f"  - Mis à jour:     {stats['updated']}")
    print(f"  - Erreurs:        {stats['errors']}")
    print(f"{'='*50}")

    return stats


if __name__ == "__main__":
    print("=" * 50)
    print("SYNC: Vérification des statuts draft orders vs Shopify")
    print("=" * 50)
    sync_all_draft_order_statuses()
