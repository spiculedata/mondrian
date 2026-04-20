#!/usr/bin/env python3
"""Parse target/foodmart/foodmart.script (HSQLDB) into:
  - scripts/postgres/foodmart-ddl.sql (Postgres CREATE TABLE + INDEX DDL)
  - target/foodmart-postgres-csv/<table>.csv (one CSV per table)

HSQLDB type map -> Postgres:
  VARCHAR_IGNORECASE(n) -> VARCHAR(n)       (case-insensitive handled via citext/collate later if needed)
  DECIMAL(p,s)          -> NUMERIC(p,s)
  TINYINT               -> SMALLINT
  DOUBLE                -> DOUBLE PRECISION
  LONGVARCHAR           -> TEXT
  BINARY                -> BYTEA
  (INTEGER, BIGINT, SMALLINT, DATE, TIME, TIMESTAMP, BOOLEAN pass through)

Value conversions for CSV:
  NULL         -> empty field (default \\N handling with NULL AS '' in COPY)
  TRUE/FALSE   -> t / f
  'O''Brien'   -> O"Brien (CSV double-quote escape)
  1.5E0        -> 1.5
  timestamps  left as-is (Postgres parses them)

Usage: python3 extract_foodmart.py <foodmart.script> <ddl_out> <csv_out_dir>
"""
import os
import re
import sys
import csv
import io
from pathlib import Path

# ----- DDL parsing -----

TYPE_MAP = [
    (re.compile(r'\bVARCHAR_IGNORECASE\b', re.I), 'VARCHAR'),
    (re.compile(r'\bLONGVARCHAR\b',        re.I), 'TEXT'),
    (re.compile(r'\bTINYINT\b',            re.I), 'SMALLINT'),
    (re.compile(r'\bDOUBLE\b',             re.I), 'DOUBLE PRECISION'),
    (re.compile(r'\bDECIMAL\b',            re.I), 'NUMERIC'),
    (re.compile(r'\bBIT\b',                re.I), 'BOOLEAN'),
    (re.compile(r'\bBINARY\b',             re.I), 'BYTEA'),
]

def translate_type(col_def: str) -> str:
    out = col_def
    for pat, rep in TYPE_MAP:
        out = pat.sub(rep, out)
    return out

CREATE_TABLE_RE = re.compile(
    r'^CREATE\s+(?:MEMORY|CACHED|GLOBAL\s+TEMPORARY|TEMP)?\s*TABLE\s+"([^"]+)"\s*\((.*)\)\s*$',
    re.I | re.S)
CREATE_INDEX_RE = re.compile(
    r'^CREATE\s+(UNIQUE\s+)?INDEX\s+"([^"]+)"\s+ON\s+"([^"]+)"\s*\(([^)]+)\)\s*$',
    re.I)

# ----- INSERT parsing -----
# Tokenizer for HSQLDB VALUES tuples.
# Handles: 'single' strings w/ '' escape, NULL, TRUE/FALSE, numbers incl. 8.39E0, signed.

def tokenize_values(s: str):
    """Tokenize contents between VALUES( ... ). Yields python values."""
    i = 0
    n = len(s)
    out = []
    while i < n:
        c = s[i]
        if c.isspace() or c == ',':
            i += 1
            continue
        if c == "'":
            # string literal, '' is escaped quote
            j = i + 1
            buf = []
            while j < n:
                if s[j] == "'":
                    if j + 1 < n and s[j+1] == "'":
                        buf.append("'")
                        j += 2
                        continue
                    else:
                        break
                buf.append(s[j])
                j += 1
            out.append(''.join(buf))
            i = j + 1
            continue
        # literal token (NULL/TRUE/FALSE/number)
        j = i
        while j < n and s[j] != ',' and not s[j].isspace():
            j += 1
        tok = s[i:j]
        u = tok.upper()
        if u == 'NULL':
            out.append(None)
        elif u == 'TRUE':
            out.append(True)
        elif u == 'FALSE':
            out.append(False)
        else:
            # numeric, possibly with E-notation, leave as string; Postgres parses fine
            out.append(tok)
        i = j
    return out

INSERT_RE = re.compile(r'^INSERT\s+INTO\s+"([^"]+)"\s+VALUES\((.*)\)\s*$', re.I | re.S)


def cell_to_csv(v):
    if v is None:
        return None   # sentinel -> empty field
    if v is True:
        return 't'
    if v is False:
        return 'f'
    return v  # string


def main():
    src = Path(sys.argv[1])
    ddl_out = Path(sys.argv[2])
    csv_dir = Path(sys.argv[3])
    csv_dir.mkdir(parents=True, exist_ok=True)
    ddl_out.parent.mkdir(parents=True, exist_ok=True)

    tables = []          # list of (name, column-list-sql)
    table_columns = {}   # name -> [col names] (quoted "x")
    indexes = []         # list of (unique, idx_name, table, cols_sql)

    # First pass: DDL only.
    with src.open('r', encoding='utf-8') as f:
        for line in f:
            line = line.rstrip('\n')
            m = CREATE_TABLE_RE.match(line)
            if m:
                name = m.group(1)
                body = m.group(2).strip()
                # split columns at top-level commas
                cols_sql = split_cols(body)
                tables.append((name, cols_sql))
                # extract column names
                col_names = []
                for c in cols_sql:
                    cm = re.match(r'\s*"([^"]+)"', c)
                    if cm:
                        col_names.append(cm.group(1))
                table_columns[name] = col_names
                continue
            mi = CREATE_INDEX_RE.match(line)
            if mi:
                indexes.append((bool(mi.group(1)), mi.group(2), mi.group(3), mi.group(4).strip()))
                continue

    # Write DDL
    with ddl_out.open('w', encoding='utf-8') as out:
        out.write('-- Generated from target/foodmart/foodmart.script\n')
        out.write('-- HSQLDB -> Postgres; VARCHAR_IGNORECASE -> VARCHAR (case-insensitive semantics not preserved).\n')
        out.write('-- Generated by scripts/postgres/extract_foodmart.py\n\n')
        out.write('SET client_min_messages = WARNING;\n\n')
        # DROP in reverse dependency-free order (no FKs enforced)
        out.write('-- Drop existing tables\n')
        for (name, _) in tables:
            out.write(f'DROP TABLE IF EXISTS "{name}" CASCADE;\n')
        out.write('\n')
        for (name, cols_sql) in tables:
            out.write(f'CREATE TABLE "{name}" (\n')
            trans = [translate_type(c.strip()) for c in cols_sql]
            out.write(',\n'.join('  ' + c for c in trans))
            out.write('\n);\n\n')
        out.write('-- Indexes\n')
        for uniq, iname, tname, cols_sql in indexes:
            u = 'UNIQUE ' if uniq else ''
            out.write(f'CREATE {u}INDEX "{iname}" ON "{tname}" ({cols_sql});\n')
        out.write('\n')

    # Second pass: INSERT rows -> per-table CSV files.
    writers = {}
    files = {}
    try:
        with src.open('r', encoding='utf-8') as f:
            for line in f:
                if not line.startswith('INSERT INTO '):
                    continue
                m = INSERT_RE.match(line.rstrip('\n'))
                if not m:
                    continue
                table = m.group(1)
                vals = tokenize_values(m.group(2))
                if table not in writers:
                    fp = (csv_dir / f'{table}.csv').open('w', encoding='utf-8', newline='')
                    files[table] = fp
                    writers[table] = csv.writer(
                        fp, quoting=csv.QUOTE_MINIMAL,
                        quotechar='"', escapechar=None, doublequote=True,
                        lineterminator='\n')
                row = []
                for v in vals:
                    cv = cell_to_csv(v)
                    # csv module writes None as '' — good for NULL
                    row.append(cv)
                writers[table].writerow(['' if r is None else r for r in row])
    finally:
        for fp in files.values():
            fp.close()

    # Print table list + row counts info for caller
    print(f'DDL -> {ddl_out}')
    print(f'CSVs -> {csv_dir}')
    for (name, _) in tables:
        p = csv_dir / f'{name}.csv'
        n_rows = 0
        if p.exists():
            with p.open('rb') as fp:
                for _ in fp:
                    n_rows += 1
        print(f'  {name}: {n_rows} rows')


def split_cols(body: str):
    """Split the columns list on commas not inside parens or quotes."""
    depth = 0
    in_str = False
    in_id = False
    parts = []
    start = 0
    for i, c in enumerate(body):
        if in_str:
            if c == "'" and (i + 1 >= len(body) or body[i+1] != "'"):
                in_str = False
            continue
        if in_id:
            if c == '"':
                in_id = False
            continue
        if c == "'":
            in_str = True
        elif c == '"':
            in_id = True
        elif c == '(':
            depth += 1
        elif c == ')':
            depth -= 1
        elif c == ',' and depth == 0:
            parts.append(body[start:i])
            start = i + 1
    parts.append(body[start:])
    return parts


if __name__ == '__main__':
    main()
