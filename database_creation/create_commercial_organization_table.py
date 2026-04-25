#!/usr/bin/env python3
"""
Migration : Création de la table de dimension `commercial_organization`.

Cette table sert de référentiel pour toutes les organisations commerciales
(pays / stores) et est référencée par les tables de faits via la colonne
`commercial_organisation` (VARCHAR(10) déjà existante).

Schéma cible :
    commercial_organization (
        commercial_organization_code  VARCHAR(10)  PRIMARY KEY,
        commercial_organization_label VARCHAR(100) NOT NULL,
        is_active                     BOOLEAN      NOT NULL DEFAULT TRUE,
        created_at                    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        updated_at                    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
    )

Population initiale :
    ('US', 'United-States')
    ('JP', 'Japon')

Usage :
    python database_creation/create_commercial_organization_table.py
    python database_creation/create_commercial_organization_table.py --sql-only
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv

load_dotenv()

LINKED_TABLES = [
    "orders",
    "orders_details",
    "transaction",
    "customers",
    "draft_order",
    "payout",
    "payout_transaction",
    "inventory",
    "inventory_history",
    "locations",
    "products",
]


def get_db_connection():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        user = os.getenv("SUPABASE_USER")
        password = os.getenv("SUPABASE_PASSWORD")
        host = os.getenv("SUPABASE_HOST")
        port = os.getenv("SUPABASE_PORT")
        dbname = os.getenv("SUPABASE_DB_NAME")
        if not all([user, password, host, port, dbname]):
            raise ValueError("Variables de connexion DB manquantes")
        db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    return psycopg2.connect(db_url)


def run_migration():
    sql_file = os.path.join(
        os.path.dirname(__file__), "create_commercial_organization_table.sql"
    )
    with open(sql_file, "r", encoding="utf-8") as f:
        sql = f.read()

    conn = get_db_connection()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        print("=" * 72)
        print("Migration : création de la table dimension commercial_organization")
        print("=" * 72)

        # État avant
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'commercial_organization'
            )
            """
        )
        already_exists = cur.fetchone()[0]
        print(
            f"\nTable commercial_organization déjà présente : {already_exists}"
        )

        cur.execute(
            """
            SELECT constraint_name, table_name
            FROM information_schema.table_constraints
            WHERE table_schema = 'public'
              AND constraint_type = 'FOREIGN KEY'
              AND constraint_name LIKE 'fk_%_commercial_organization'
            ORDER BY table_name
            """
        )
        existing_fks = cur.fetchall()
        print(f"FKs existantes vers commercial_organization : {len(existing_fks)}")
        for fk_name, tbl in existing_fks:
            print(f"  - {tbl} : {fk_name}")

        print("\nExécution du SQL de migration...")
        # psycopg2 gère la transaction, on retire les BEGIN/COMMIT explicites
        sql_clean = sql.replace("BEGIN;", "").replace("COMMIT;", "")
        cur.execute(sql_clean)
        conn.commit()
        print("SQL appliqué avec succès.")

        # État après
        print("\nContenu de commercial_organization :")
        cur.execute(
            """
            SELECT commercial_organization_code, commercial_organization_label, is_active
            FROM commercial_organization
            ORDER BY commercial_organization_code
            """
        )
        for row in cur.fetchall():
            print(f"  {row[0]:<4} → {row[1]:<20} (active={row[2]})")

        print("\nClés étrangères créées :")
        cur.execute(
            """
            SELECT tc.table_name, tc.constraint_name
            FROM information_schema.table_constraints tc
            WHERE tc.table_schema = 'public'
              AND tc.constraint_type = 'FOREIGN KEY'
              AND tc.constraint_name LIKE 'fk_%_commercial_organization'
            ORDER BY tc.table_name
            """
        )
        rows = cur.fetchall()
        for tbl, fk_name in rows:
            print(f"  [OK] {tbl:<22} → {fk_name}")

        missing = set(LINKED_TABLES) - {tbl for tbl, _ in rows}
        if missing:
            print(f"\nATTENTION : FK manquantes pour : {sorted(missing)}")

        print("\n" + "=" * 72)
        print("Migration terminée avec succès !")
        print("=" * 72)

    except Exception as exc:
        conn.rollback()
        print(f"\nERREUR lors de la migration : {exc}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


def print_sql():
    sql_file = os.path.join(
        os.path.dirname(__file__), "create_commercial_organization_table.sql"
    )
    with open(sql_file, "r", encoding="utf-8") as f:
        print(f.read())


if __name__ == "__main__":
    if "--sql-only" in sys.argv:
        print_sql()
    else:
        run_migration()
