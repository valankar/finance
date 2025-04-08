#!/usr/bin/env python3

import duckdb

conn = duckdb.connect(":memory:")
conn.execute("ATTACH 'web/db.duckdb' as duck_db")
conn.execute("ATTACH 'web/sqlite.db' as sqlite_db")
conn.execute("COPY FROM DATABASE sqlite_db TO duck_db")
