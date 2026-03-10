#!/usr/bin/env python3
"""
Migration : Ajout des colonnes multi-pays à toutes les tables.
  - data_source           (ex: 'Shopify')
  - company_code          (ex: 'ADAM_LIPPES')
  - commercial_organisation (US, JP, FR, UK)

Usage:
    python database_creation/add_multi_country_columns.py
    python database_creation/add_multi_country_columns.py --sql-only   # affiche le SQL sans l'exécuter
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv

load_dotenv()

TABLES = [
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

COLUMNS = [
    ("data_source", "VARCHAR(50)", "Shopify"),
    ("company_code", "VARCHAR(50)", "ADAM_LIPPES"),
    ("commercial_organisation", "VARCHAR(10)", "US"),
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
    sql_file = os.path.join(os.path.dirname(__file__), "add_multi_country_columns.sql")
    with open(sql_file, "r", encoding="utf-8") as f:
        sql = f.read()

    conn = get_db_connection()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        print("=" * 60)
        print("Migration multi-pays : ajout de data_source, company_code, commercial_organisation")
        print("=" * 60)

        # Vérifier l'état avant migration
        print("\nÉtat avant migration :")
        for table in TABLES:
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = %s AND column_name IN ('data_source', 'company_code', 'commercial_organisation')
                """,
                (table,),
            )
            existing = [row[0] for row in cur.fetchall()]
            if len(existing) == 3:
                print(f"  {table}: colonnes déjà présentes (skip possible)")
            elif existing:
                print(f"  {table}: colonnes partielles {existing}")
            else:
                print(f"  {table}: aucune colonne multi-pays")

        print("\nExécution du SQL de migration...")

        # Le fichier SQL contient BEGIN/COMMIT, on exécute directement
        # mais psycopg2 gère les transactions, donc on enlève les BEGIN/COMMIT explicites
        sql_clean = sql.replace("BEGIN;", "").replace("COMMIT;", "")
        cur.execute(sql_clean)
        conn.commit()

        # Vérifier l'état après migration
        print("\nÉtat après migration :")
        all_ok = True
        for table in TABLES:
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = %s AND column_name IN ('data_source', 'company_code', 'commercial_organisation')
                ORDER BY column_name
                """,
                (table,),
            )
            existing = [row[0] for row in cur.fetchall()]
            status = "OK" if len(existing) == 3 else "ERREUR"
            if status == "ERREUR":
                all_ok = False
            print(f"  [{status}] {table}: {existing}")

        # Compter les lignes par table pour vérifier les données
        print("\nNombre de lignes par table :")
        for table in TABLES:
            try:
                cur.execute(f'SELECT COUNT(*) FROM "{table}"')
                count = cur.fetchone()[0]
                cur.execute(
                    f"SELECT COUNT(*) FROM \"{table}\" WHERE commercial_organisation = 'US'"
                )
                count_us = cur.fetchone()[0]
                print(f"  {table}: {count} lignes ({count_us} US)")
            except Exception as e:
                print(f"  {table}: erreur de comptage ({e})")
                conn.rollback()

        print("\n" + "=" * 60)
        if all_ok:
            print("Migration terminée avec succès !")
        else:
            print("Migration terminée avec des erreurs. Vérifier les tables ci-dessus.")
        print("=" * 60)

    except Exception as e:
        conn.rollback()
        print(f"\nERREUR lors de la migration : {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


def print_sql():
    sql_file = os.path.join(os.path.dirname(__file__), "add_multi_country_columns.sql")
    with open(sql_file, "r", encoding="utf-8") as f:
        print(f.read())


if __name__ == "__main__":
    if "--sql-only" in sys.argv:
        print_sql()
    else:
        run_migration()
