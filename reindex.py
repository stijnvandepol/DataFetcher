#!/usr/bin/env python3
"""
Rebuild search indexes for existing tables with proper error handling.
Run inside the fetcher container or with access to the database.
"""

import os
import sys
import hashlib
import psycopg2
import psycopg2.sql as sql

DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

SEARCH_INDEX_COLUMNS = [
    "Name",
    "vlocity_cmt__BillingEmailAddress__c",
    "Phone",
    "BillingCity",
    "BillingPostalCode",
    "BillingStreet",
    "House_Number__c",
    "Bank_Account_Number__c",
    "Bank_Account_Holder_Name__c",
    "Id",
    "Account_Salesforce_ID__c",
    "ParentAccountName__c",
    "Description",
    "Flash_Message__c",
]


def _idx_name(table_name, col_name, kind):
    digest = hashlib.md5(f"{table_name}:{col_name}:{kind}".encode()).hexdigest()[:10]
    return f"idx_{table_name}_{kind}_{digest}"


def rebuild_indexes_for_table(conn, table_name):
    """Rebuild search indexes for a single table."""
    print(f"\n=== Processing table: {table_name} ===")
    cur = conn.cursor()
    
    try:
        # Ensure pg_trgm extension exists
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        conn.commit()
        
        # Get existing columns
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = %s",
            (table_name,),
        )
        existing = {row[0] for row in cur.fetchall()}
        
        success_count = 0
        skip_count = 0
        
        for col in SEARCH_INDEX_COLUMNS:
            if col not in existing:
                continue
            
            trigram_idx = _idx_name(table_name, col, "trgm")
            prefix_idx = _idx_name(table_name, col, "lower")
            
            # Drop old indexes if they exist (to recreate cleanly)
            try:
                cur.execute(sql.SQL("DROP INDEX IF EXISTS {}").format(sql.Identifier(trigram_idx)))
                cur.execute(sql.SQL("DROP INDEX IF EXISTS {}").format(sql.Identifier(prefix_idx)))
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"  Warning: could not drop old indexes for {col}: {e}")
            
            # Create trigram index (GIN - usually succeeds)
            try:
                print(f"  Creating trigram index on {col}...", end=" ")
                cur.execute(
                    sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} USING GIN ({} gin_trgm_ops)").format(
                        sql.Identifier(trigram_idx),
                        sql.Identifier(table_name),
                        sql.Identifier(col),
                    )
                )
                conn.commit()
                print("✓")
                success_count += 1
            except Exception as e:
                conn.rollback()
                error_msg = str(e).split("\n")[0]
                print(f"✗ ({error_msg})")
            
            # Create LOWER() index (B-tree - may fail on large values)
            try:
                print(f"  Creating LOWER() index on {col}...", end=" ")
                cur.execute(
                    sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ((LOWER({})))").format(
                        sql.Identifier(prefix_idx),
                        sql.Identifier(table_name),
                        sql.Identifier(col),
                    )
                )
                conn.commit()
                print("✓")
                success_count += 1
            except Exception as e:
                conn.rollback()
                error_msg = str(e)
                if "exceeds btree" in error_msg or "cannot be indexed" in error_msg:
                    print(f"⊘ (value too large, skipped)")
                    skip_count += 1
                else:
                    error_line = error_msg.split("\n")[0]
                    print(f"✗ ({error_line})")
        
        print(f"  Summary: {success_count} indexes created, {skip_count} skipped due to size limitations")
        
    except Exception as e:
        conn.rollback()
        print(f"  ERROR processing {table_name}: {e}")
    finally:
        cur.close()


def main():
    print("Connecting to database...")
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    print("Connected!")
    
    # Get all data tables
    cur = conn.cursor()
    cur.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public' AND "
        "(table_name LIKE 'accounts_%' OR table_name LIKE 'data_%')"
    )
    tables = [row[0] for row in cur.fetchall()]
    cur.close()
    
    if not tables:
        print("No data tables found!")
        return
    
    print(f"Found {len(tables)} table(s) to process:")
    for t in tables:
        print(f"  - {t}")
    
    for table in tables:
        rebuild_indexes_for_table(conn, table)
    
    conn.close()
    print("\n✓ All done!")


if __name__ == "__main__":
    main()
