#!/usr/bin/env python3
"""
Generate a synthetic duperemove hashfile DB and benchmark duplicate-finding queries.

Creates a realistic SQLite database matching duperemove's schema with configurable
duplicate patterns, then benchmarks the three duplicate queries (files, extents,
blocks) using both the current materialized-JOIN approach and the original
nested-IN approach.
"""

import argparse
import math
import os
import random
import sqlite3
import sys
import time

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BLOCKSIZE = 128 * 1024  # 128 KB
PAGE_SIZE = 4096
DIGEST_LEN = 16

# ---------------------------------------------------------------------------
# Schema & index DDL (matches dbfile.c)
# ---------------------------------------------------------------------------

SCHEMA_SQL = [
    """CREATE TABLE IF NOT EXISTS config(
        keyname TEXT PRIMARY KEY NOT NULL,
        keyval BLOB,
        UNIQUE(keyname))""",
    """CREATE TABLE IF NOT EXISTS files(
        id INTEGER PRIMARY KEY NOT NULL,
        filename TEXT NOT NULL,
        ino INTEGER, subvol INTEGER, size INTEGER,
        mtime INTEGER, dedupe_seq INTEGER, digest BLOB,
        flags INTEGER,
        UNIQUE(ino, subvol), UNIQUE(filename))""",
    """CREATE TABLE IF NOT EXISTS extents(
        digest BLOB KEY NOT NULL,
        fileid INTEGER, loff INTEGER, poff INTEGER, len INTEGER,
        UNIQUE(fileid, loff, len),
        FOREIGN KEY(fileid) REFERENCES files(id) ON DELETE CASCADE)""",
    """CREATE TABLE IF NOT EXISTS blocks(
        digest BLOB KEY NOT NULL,
        fileid INTEGER, loff INTEGER,
        UNIQUE(fileid, loff),
        FOREIGN KEY(fileid) REFERENCES files(id) ON DELETE CASCADE)""",
]

INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_blocks_digest ON blocks(digest)",
    "CREATE INDEX IF NOT EXISTS idx_blocks_fileid ON blocks(fileid)",
    "CREATE INDEX IF NOT EXISTS idx_extents_digest_len ON extents(digest, len)",
    "CREATE INDEX IF NOT EXISTS idx_extents_fileid ON extents(fileid)",
    "CREATE INDEX IF NOT EXISTS idx_files_ino_subvol ON files(ino, subvol)",
    "CREATE INDEX IF NOT EXISTS idx_files_dedupeseq ON files(dedupe_seq)",
    "CREATE INDEX IF NOT EXISTS idx_files_digest_size ON files(digest, size)",
]

# ---------------------------------------------------------------------------
# Queries: current (materialized JOIN) vs original (nested IN)
# ---------------------------------------------------------------------------

QUERIES_JOIN = {
    "GET_DUPLICATE_FILES": """\
SELECT f.id, f.size, f.digest, f.filename FROM files f
JOIN (
    SELECT digest, size FROM files
    WHERE dedupe_seq <= ?1 AND NOT (flags & 1)
    GROUP BY digest, size
    HAVING count(*) > 1 AND sum(dedupe_seq = ?1) > 0
) dup ON f.digest = dup.digest AND f.size = dup.size
WHERE f.dedupe_seq <= ?1 AND NOT (f.flags & 1)""",
    "GET_DUPLICATE_EXTENTS": """\
SELECT e.digest, e.fileid, e.loff, e.len, e.poff FROM extents e
JOIN files f ON e.fileid = f.id
JOIN (
    SELECT ex.digest, ex.len FROM extents ex
    JOIN files fx ON ex.fileid = fx.id
    WHERE fx.dedupe_seq <= ?1
    GROUP BY ex.digest, ex.len
    HAVING count(*) > 1 AND sum(fx.dedupe_seq = ?1) > 0
) dup ON e.digest = dup.digest AND e.len = dup.len
WHERE f.dedupe_seq <= ?1""",
    "GET_DUPLICATE_BLOCKS": """\
SELECT b.digest, b.fileid, b.loff FROM blocks b
JOIN files f ON b.fileid = f.id
JOIN (
    SELECT bx.digest FROM blocks bx
    JOIN files fx ON bx.fileid = fx.id
    WHERE fx.dedupe_seq <= ?1
    GROUP BY bx.digest
    HAVING count(*) > 1 AND sum(fx.dedupe_seq = ?1) > 0
) dup ON b.digest = dup.digest
WHERE f.dedupe_seq <= ?1""",
}

QUERIES_NESTED_IN = {
    "GET_DUPLICATE_FILES": """\
SELECT id, size, digest, filename FROM files
WHERE dedupe_seq <= ?1 AND NOT (flags & 1) AND (digest, size) IN (
    SELECT digest, size FROM files
    WHERE dedupe_seq <= ?1 AND NOT (flags & 1) AND (digest, size) IN (
        SELECT digest, size FROM files
        WHERE dedupe_seq = ?1 AND NOT (flags & 1))
    GROUP BY digest, size HAVING count(*) > 1)""",
    "GET_DUPLICATE_EXTENTS": """\
SELECT extents.digest, fileid, loff, len, poff FROM extents
JOIN files ON fileid = id
WHERE dedupe_seq <= ?1 AND (extents.digest, len) IN (
    SELECT extents.digest, len FROM extents
    JOIN files ON fileid = id
    WHERE dedupe_seq <= ?1 AND (extents.digest, len) IN (
        SELECT extents.digest, len FROM extents
        JOIN files ON fileid = id
        WHERE dedupe_seq = ?1)
    GROUP BY extents.digest, len HAVING count(*) > 1)""",
    "GET_DUPLICATE_BLOCKS": """\
SELECT blocks.digest, fileid, loff FROM blocks
JOIN files ON fileid = id
WHERE dedupe_seq <= ?1 AND blocks.digest IN (
    SELECT blocks.digest FROM blocks
    JOIN files ON fileid = id
    WHERE dedupe_seq <= ?1 AND blocks.digest IN (
        SELECT blocks.digest FROM blocks
        JOIN files ON fileid = id
        WHERE dedupe_seq = ?1)
    GROUP BY blocks.digest HAVING count(*) > 1)""",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_size(s):
    """Parse human-readable size string (e.g. '100G', '1T') to bytes."""
    s = s.strip().upper()
    multipliers = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
    if s[-1] in multipliers:
        return int(float(s[:-1]) * multipliers[s[-1]])
    return int(s)


def fmt_size(n):
    """Format bytes to human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def align_down(v, alignment):
    return (v // alignment) * alignment


def align_up(v, alignment):
    return ((v + alignment - 1) // alignment) * alignment


def fmt_duration(seconds):
    if seconds < 1:
        return f"{seconds*1000:.1f} ms"
    if seconds < 60:
        return f"{seconds:.2f} s"
    m, s = divmod(seconds, 60)
    return f"{int(m)}m {s:.1f}s"


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------

def generate_db(args):
    db_path = args.output
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Performance pragmas for bulk insert
    cur.execute("PRAGMA synchronous = OFF")
    cur.execute("PRAGMA journal_mode = WAL")
    cur.execute("PRAGMA cache_size = -256000")
    cur.execute("PRAGMA foreign_keys = OFF")

    # Create tables (no indexes yet)
    for sql in SCHEMA_SQL:
        cur.execute(sql)
    conn.commit()

    rng = random.Random(args.seed)
    target_bytes = parse_size(args.target_size)
    num_files = args.num_seq * args.batch_size

    print(f"Target: {fmt_size(target_bytes)}, {num_files} files "
          f"({args.num_seq} seqs × {args.batch_size} batch)")

    # --- Generate file sizes (log-normal, aligned to PAGE_SIZE) ---
    t0 = time.monotonic()
    print("Generating file sizes...", end=" ", flush=True)

    # Log-normal with median ~600KB gives a realistic mix of small and large files
    raw_sizes = [max(PAGE_SIZE, rng.lognormvariate(mu=13.3, sigma=1.2))
                 for _ in range(num_files)]
    # Scale to hit target total
    total_raw = sum(raw_sizes)
    scale = target_bytes / total_raw
    file_sizes = [align_up(max(PAGE_SIZE, int(s * scale)), PAGE_SIZE)
                  for s in raw_sizes]
    actual_total = sum(file_sizes)
    print(f"done. Actual total: {fmt_size(actual_total)}")

    # --- Prepare duplicate groups ---
    print("Preparing duplicate patterns...")

    wide_groups = args.wide_groups
    wide_copies = args.wide_copies
    narrow_groups = args.narrow_groups
    narrow_copies = args.narrow_copies

    total_dup_files = wide_groups * wide_copies + narrow_groups * narrow_copies
    if total_dup_files > num_files:
        print(f"WARNING: duplicate files ({total_dup_files}) exceed total files "
              f"({num_files}), reducing groups")
        wide_groups = min(wide_groups, num_files // (wide_copies + narrow_copies + 1))
        narrow_groups = min(narrow_groups, (num_files - wide_groups * wide_copies) // narrow_copies)
        total_dup_files = wide_groups * wide_copies + narrow_groups * narrow_copies

    # Pre-generate digests for duplicate groups
    # Wide file dups: same (digest, size) across many copies
    wide_file_digests = [rng.randbytes(DIGEST_LEN) for _ in range(wide_groups)]
    wide_file_sizes_pool = rng.sample(range(num_files), min(wide_groups, num_files))

    # Narrow file dups: same (digest, size) with just 2 copies
    narrow_file_digests = [rng.randbytes(DIGEST_LEN) for _ in range(narrow_groups)]

    # Build assignment: which file indices get which duplicate digest
    # Spread across dedupe_seq batches for realism
    file_digest = [None] * num_files  # None = unique
    file_flags = [0] * num_files

    # Mark ~0.1% as FILE_INLINED
    num_inlined = max(1, num_files // 1000)
    inlined_indices = set(rng.sample(range(num_files), num_inlined))
    for idx in inlined_indices:
        file_flags[idx] = 1

    # Assign wide duplicates: pick random file indices spread across batches
    available = list(range(num_files))
    rng.shuffle(available)
    pos = 0
    wide_group_members = []
    for g in range(wide_groups):
        members = available[pos:pos + wide_copies]
        pos += wide_copies
        # All members share same digest and size
        digest = wide_file_digests[g]
        shared_size = file_sizes[members[0]]
        for idx in members:
            file_digest[idx] = digest
            file_sizes[idx] = shared_size  # force same size for file-level dup
        wide_group_members.append(members)

    # Assign narrow duplicates
    narrow_group_members = []
    for g in range(narrow_groups):
        members = available[pos:pos + narrow_copies]
        pos += narrow_copies
        digest = narrow_file_digests[g]
        shared_size = file_sizes[members[0]]
        for idx in members:
            file_digest[idx] = digest
            file_sizes[idx] = shared_size
        narrow_group_members.append(members)

    # Fill unique digests
    for i in range(num_files):
        if file_digest[i] is None:
            file_digest[i] = rng.randbytes(DIGEST_LEN)

    print(f"  Wide dup groups: {wide_groups} × {wide_copies} copies = "
          f"{wide_groups * wide_copies} files")
    print(f"  Narrow dup groups: {narrow_groups} × {narrow_copies} copies = "
          f"{narrow_groups * narrow_copies} files")
    print(f"  Inlined files: {num_inlined}")

    # --- Extent-level duplicate preparation ---
    num_extent_dup_groups = 2000
    extent_dup_digests = [(rng.randbytes(DIGEST_LEN), align_up(rng.randint(PAGE_SIZE, 512 * 1024), PAGE_SIZE))
                          for _ in range(num_extent_dup_groups)]
    # Each group appears in 3-20 files; we'll inject these during extent generation
    extent_dup_assignments = {}  # file_idx -> list of (digest, len) to inject
    for digest, length in extent_dup_digests:
        n_copies = rng.randint(3, 20)
        targets = rng.sample(range(num_files), min(n_copies, num_files))
        for idx in targets:
            extent_dup_assignments.setdefault(idx, []).append((digest, length))

    # --- Block-level duplicate preparation ---
    num_block_dup_groups = 5000
    block_dup_digests = [rng.randbytes(DIGEST_LEN) for _ in range(num_block_dup_groups)]
    block_dup_assignments = {}  # file_idx -> list of digests to inject
    for digest in block_dup_digests:
        n_copies = rng.randint(5, 50)
        targets = rng.sample(range(num_files), min(n_copies, num_files))
        for idx in targets:
            block_dup_assignments.setdefault(idx, []).append(digest)

    # --- Insert files ---
    print("Inserting files...", end=" ", flush=True)
    t_files = time.monotonic()

    BATCH = 50000
    file_rows = []
    for i in range(num_files):
        seq = i // args.batch_size + 1
        file_rows.append((
            f"/mnt/data/dir{i % 1000:04d}/file_{i:08d}",  # filename
            i + 1,           # ino
            1,               # subvol
            file_sizes[i],   # size
            1700000000 + i,  # mtime
            seq,             # dedupe_seq
            file_digest[i],  # digest
            file_flags[i],   # flags
        ))
        if len(file_rows) >= BATCH:
            cur.executemany(
                "INSERT INTO files(filename,ino,subvol,size,mtime,dedupe_seq,digest,flags) "
                "VALUES(?,?,?,?,?,?,?,?)", file_rows)
            conn.commit()
            file_rows.clear()

    if file_rows:
        cur.executemany(
            "INSERT INTO files(filename,ino,subvol,size,mtime,dedupe_seq,digest,flags) "
            "VALUES(?,?,?,?,?,?,?,?)", file_rows)
        conn.commit()
        file_rows.clear()

    print(f"done ({fmt_duration(time.monotonic() - t_files)})")

    # --- Insert extents ---
    print("Inserting extents...", end=" ", flush=True)
    t_extents = time.monotonic()

    extent_rows = []
    total_extents = 0
    extent_weights = [1, 2, 2, 3]  # weighted towards 2 extents per file

    for i in range(num_files):
        fsize = file_sizes[i]
        fileid = i + 1  # SQLite rowid starts at 1

        # Determine number of extents (1-3, weighted)
        n_extents = rng.choice(extent_weights)
        if fsize <= PAGE_SIZE:
            n_extents = 1

        # Split file into n_extents contiguous regions
        if n_extents == 1:
            lengths = [fsize]
        else:
            # Generate split points
            splits = sorted(rng.sample(
                range(PAGE_SIZE, fsize, PAGE_SIZE),
                min(n_extents - 1, max(0, fsize // PAGE_SIZE - 1))
            ))
            lengths = []
            prev = 0
            for sp in splits:
                lengths.append(sp - prev)
                prev = sp
            lengths.append(fsize - prev)

        # Check for injected extent-level duplicates
        injected = extent_dup_assignments.get(i, [])
        injected_idx = 0

        loff = 0
        for ext_i, ext_len in enumerate(lengths):
            poff = rng.randint(0, 2**40) & ~(PAGE_SIZE - 1)

            # Maybe use an injected duplicate digest for this extent
            if injected_idx < len(injected) and ext_i == 0:
                dup_digest, dup_len = injected[injected_idx]
                injected_idx += 1
                # Use the dup digest but keep original loff; override length
                extent_rows.append((dup_digest, fileid, loff, poff, dup_len))
                loff += dup_len
            else:
                ext_digest = rng.randbytes(DIGEST_LEN)
                extent_rows.append((ext_digest, fileid, loff, poff, ext_len))
                loff += ext_len

            total_extents += 1

        if len(extent_rows) >= BATCH:
            cur.executemany(
                "INSERT OR IGNORE INTO extents(digest,fileid,loff,poff,len) "
                "VALUES(?,?,?,?,?)", extent_rows)
            conn.commit()
            extent_rows.clear()

    if extent_rows:
        cur.executemany(
            "INSERT OR IGNORE INTO extents(digest,fileid,loff,poff,len) "
            "VALUES(?,?,?,?,?)", extent_rows)
        conn.commit()
        extent_rows.clear()

    print(f"done. {total_extents} extents ({fmt_duration(time.monotonic() - t_extents)})")

    # --- Insert blocks ---
    if not args.skip_blocks:
        print("Inserting blocks...", end=" ", flush=True)
        t_blocks = time.monotonic()

        block_rows = []
        total_blocks = 0

        for i in range(num_files):
            fsize = file_sizes[i]
            fileid = i + 1
            n_blocks = math.ceil(fsize / BLOCKSIZE)

            # Check for injected block-level duplicates
            injected = block_dup_assignments.get(i, [])

            for b in range(n_blocks):
                loff = b * BLOCKSIZE
                if b < len(injected):
                    block_digest = injected[b]
                else:
                    block_digest = rng.randbytes(DIGEST_LEN)

                block_rows.append((block_digest, fileid, loff))
                total_blocks += 1

            if len(block_rows) >= BATCH:
                cur.executemany(
                    "INSERT OR IGNORE INTO blocks(digest,fileid,loff) "
                    "VALUES(?,?,?)", block_rows)
                conn.commit()
                block_rows.clear()

        if block_rows:
            cur.executemany(
                "INSERT OR IGNORE INTO blocks(digest,fileid,loff) "
                "VALUES(?,?,?)", block_rows)
            conn.commit()
            block_rows.clear()

        print(f"done. {total_blocks} blocks ({fmt_duration(time.monotonic() - t_blocks)})")
    else:
        total_blocks = 0
        print("Skipping blocks (--skip-blocks)")

    # --- Create indexes ---
    print("Creating indexes...", end=" ", flush=True)
    t_idx = time.monotonic()
    for sql in INDEX_SQL:
        cur.execute(sql)
    conn.commit()
    print(f"done ({fmt_duration(time.monotonic() - t_idx)})")

    # --- Store config ---
    max_seq = args.num_seq
    cur.execute("INSERT OR REPLACE INTO config(keyname, keyval) VALUES('hash_type', 'xxhash')")
    cur.execute("INSERT OR REPLACE INTO config(keyname, keyval) VALUES('block_size', ?)",
                (str(BLOCKSIZE),))
    cur.execute("INSERT OR REPLACE INTO config(keyname, keyval) VALUES('dedupe_seq', ?)",
                (str(max_seq),))
    conn.commit()

    # --- Summary ---
    total_time = time.monotonic() - t0
    db_size = os.path.getsize(db_path)

    # Get actual row counts
    n_files = cur.execute("SELECT count(*) FROM files").fetchone()[0]
    n_extents = cur.execute("SELECT count(*) FROM extents").fetchone()[0]
    n_blocks = cur.execute("SELECT count(*) FROM blocks").fetchone()[0] if not args.skip_blocks else 0

    print(f"\n{'='*60}")
    print(f"Database: {db_path}")
    print(f"DB size:  {fmt_size(db_size)}")
    print(f"Files:    {n_files:,}")
    print(f"Extents:  {n_extents:,}")
    print(f"Blocks:   {n_blocks:,}")
    print(f"Max seq:  {max_seq}")
    print(f"Total generation time: {fmt_duration(total_time)}")
    print(f"{'='*60}\n")

    conn.close()
    return max_seq


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------

def run_benchmark(db_path, max_seq):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA cache_size = -256000")
    conn.execute("PRAGMA mmap_size = 1073741824")  # 1 GB mmap

    seq_values = [max_seq, max(1, max_seq // 2), 1]

    query_sets = [
        ("JOIN (current)", QUERIES_JOIN),
        ("Nested IN (original)", QUERIES_NESTED_IN),
    ]

    # Print EXPLAIN QUERY PLAN for each query at max_seq
    print("EXPLAIN QUERY PLAN")
    print("=" * 70)
    for set_name, queries in query_sets:
        print(f"\n--- {set_name} ---")
        for qname, qsql in queries.items():
            print(f"\n  {qname}:")
            try:
                plan = conn.execute(f"EXPLAIN QUERY PLAN {qsql}", (max_seq,)).fetchall()
                for row in plan:
                    indent = "    " + "  " * row[1]
                    print(f"{indent}{row[3]}")
            except Exception as e:
                print(f"    ERROR: {e}")
    print()

    # Run benchmarks
    print("BENCHMARK RESULTS")
    print("=" * 70)
    header = f"{'Query':<25} {'Variant':<20} {'Seq':>6} {'Rows':>10} {'Time':>12}"
    print(header)
    print("-" * 70)

    results = {}  # (qname, set_name, seq) -> sorted rows for verification

    for set_name, queries in query_sets:
        for qname in ["GET_DUPLICATE_FILES", "GET_DUPLICATE_EXTENTS", "GET_DUPLICATE_BLOCKS"]:
            qsql = queries[qname]
            for seq in seq_values:
                # Warm up: drop OS caches by re-reading (SQLite internal cache already set)
                conn.execute("SELECT 1")

                t0 = time.monotonic()
                try:
                    rows = conn.execute(qsql, (seq,)).fetchall()
                    elapsed = time.monotonic() - t0
                    n_rows = len(rows)

                    # Store sorted results for verification
                    key = (qname, seq)
                    sorted_rows = sorted(rows)
                    if key not in results:
                        results[key] = (set_name, sorted_rows)
                    else:
                        # Compare with first variant
                        prev_name, prev_rows = results[key]
                        if sorted_rows == prev_rows:
                            match = "MATCH"
                        else:
                            match = f"MISMATCH (prev={len(prev_rows)})"
                        n_rows = f"{n_rows} {match}"
                except Exception as e:
                    elapsed = time.monotonic() - t0
                    n_rows = f"ERR: {e}"

                print(f"{qname:<25} {set_name:<20} {seq:>6} {str(n_rows):>10} "
                      f"{fmt_duration(elapsed):>12}")

    print()
    conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate a synthetic duperemove hashfile DB and benchmark queries")
    parser.add_argument("-o", "--output", default="benchmark.db",
                        help="Output database path (default: benchmark.db)")
    parser.add_argument("--target-size", default="1T",
                        help="Target total file size, e.g. 100G, 1T (default: 1T)")
    parser.add_argument("--num-seq", type=int, default=1665,
                        help="Number of dedupe_seq batches (default: 1665)")
    parser.add_argument("--batch-size", type=int, default=1024,
                        help="Files per dedupe_seq batch (default: 1024)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility (default: 42)")
    parser.add_argument("--wide-groups", type=int, default=50,
                        help="Number of wide duplicate groups (default: 50)")
    parser.add_argument("--wide-copies", type=int, default=200,
                        help="Copies per wide duplicate group (default: 200)")
    parser.add_argument("--narrow-groups", type=int, default=5000,
                        help="Number of narrow duplicate groups (default: 5000)")
    parser.add_argument("--narrow-copies", type=int, default=2,
                        help="Copies per narrow duplicate group (default: 2)")
    parser.add_argument("--skip-blocks", action="store_true",
                        help="Skip block generation (faster for file/extent-only testing)")
    parser.add_argument("--skip-benchmark", action="store_true",
                        help="Skip benchmark after generation")

    args = parser.parse_args()
    max_seq = generate_db(args)

    if not args.skip_benchmark:
        run_benchmark(args.output, max_seq)


if __name__ == "__main__":
    main()
