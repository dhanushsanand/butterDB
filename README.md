# ButterDB — B-tree storage engine

A multi-threaded, persistent key-value store written in C11, built on a B+tree
with its own paging engine, buffer pool, write-ahead log and fine-grained
latching. No dependencies beyond POSIX and pthreads.

```
$ make && make test
test_basic: PASS
test_wal: PASS
test_concurrent: PASS

$ ./bench bench.db 200000 8
put:  1.43s  139960 ops/s
get:  0.39s  516965 ops/s
```
(8 threads, 200k keys, WAL fsync off. With `sync_wal=1` writes are bounded by fsync latency.)

## Architecture

```
 butterdb_put / get / delete / scan              public API   (include/butterdb.h)
          │
          ▼
   ┌─────────────┐   WAL append + fsync BEFORE the tree is touched
   │   db.c      │──────────────────────────────────────────▶  wal.c   <db>.wal
   │ checkpoints │
   └─────────────┘
          │
          ▼
   ┌─────────────┐   B+tree, latch crabbing, split propagation
   │  btree.c    │
   └─────────────┘
          │  pin / latch / unpin
          ▼
   ┌─────────────┐   LRU cache, hash index, no-steal policy,
   │ bufpool.c   │   per-frame rwlock latches, double-write checkpoint journal
   └─────────────┘
          │  read / write / alloc page
          ▼
   ┌─────────────┐   4 KiB pages over pread/pwrite
   │  pager.c    │                                            <db>   (page 0 = meta)
   └─────────────┘
```

### Pager (`src/pager.c`)
Fixed 4 KiB pages addressed by page number. Page 0 is the meta page
(magic, version, root page, page count); everything else is a tree node.

### Buffer pool (`src/bufpool.c`)
* Frames are located with an O(1) chained hash (pgno → frame); eviction is
  LRU among clean, unpinned frames.
* Each frame carries a `pthread_rwlock_t` **latch** that protects page
  contents. The pool mutex only protects the frame table and is held briefly.
* **No-steal / force-at-checkpoint**: dirty pages are never written outside a
  checkpoint. Between checkpoints the data file is therefore *exactly* the
  last checkpointed state, which is what makes logical WAL replay correct.
* **Atomic checkpoint** via a double-write journal (`<db>.ckpt`): dirty pages
  and the meta page are first written and fsync'd to the journal, then to the
  data file, then the journal is deleted. A crash mid-checkpoint is repaired
  (journal complete → re-apply) or discarded (journal torn → data file was
  never touched) on the next open.
* If a writer finds no clean frame it returns `EBUSY`; the db layer
  checkpoints and retries. A proactive checkpoint fires at 75% dirty.

### Write-ahead log (`src/wal.c`)
Logical records `{op, key, value}`, each with a CRC-32 and length, appended
(and optionally fsync'd) before the B-tree is modified. On open, every intact
record since the last checkpoint is replayed (replay is idempotent); a torn
tail is detected by CRC/length and truncated.

### B+tree (`src/btree.c`)
* Leaf page: 25 × (32-byte key, 128-byte value slot). Internal page: 113 keys
  / 114 children. Leaves are chained for ordered scans.
* **Latch crabbing** (lock coupling):
  * readers take read latches top-down, releasing the parent once the child
    is latched;
  * `put` takes write latches top-down and keeps a stack of ancestors, but
    releases *all* ancestors the moment it reaches a node that cannot split
    (a split can never propagate past a non-full node);
  * `delete` is lazy (no merge), so it only write-latches the leaf;
  * the root pointer is guarded by a root latch that acts as the root's
    virtual parent.
* Before a split, every page the split chain will need (plus a new root if
  required) is reserved up front, so pool exhaustion can never leave the
  tree half-restructured.

## API

```c
#include "butterdb.h"

butterdb_t *db;
bdb_options_t o = { .pool_frames = 1024, .sync_wal = 1 };
butterdb_open(&db, "data.db", &o);

butterdb_put(db, "user:42", 7, "alice", 5);

char buf[128]; size_t n = sizeof buf;
if (butterdb_get(db, "user:42", 7, buf, &n) == BDB_OK) printf("%.*s\n", (int)n, buf);

butterdb_delete(db, "user:42", 7);
butterdb_scan(db, "user:", 5, my_callback, NULL);   /* ordered, from first key >= start */
butterdb_checkpoint(db);
butterdb_close(db);
```

Limits: keys 1–32 bytes (no NUL), values 0–126 bytes.

## Building

```
make            # libbutterdb.a, ./butterdb (CLI), ./bench
make test       # correctness, crash-recovery and concurrency tests
make tsan       # tests under ThreadSanitizer
make asan       # tests under Address/UB sanitizer
```

CLI:
```
$ ./butterdb demo.db
butter> put apple red
butter> get apple
red
butter> scan
butter> stats
```

## Tests
| test | what it proves |
|---|---|
| `test_basic` | 20k scrambled inserts, overwrites, deletes, ordered + ranged scans, argument validation, persistence across reopen, eviction under a 64-frame pool |
| `test_wal` | writes after a checkpoint survive `butterdb_crash()` (nothing flushed) via WAL replay; a torn WAL tail is ignored; WAL is truncated after recovery |
| `test_concurrent` | 8 writers × 10k keys with interleaved deletes, 4 concurrent readers/scanners, full verification of final state |

## Known limitations / future work
* Deletes do not merge or rebalance nodes; empty leaves are left in place.
* No free-page list: pages reserved for a split that later fails are leaked.
* Cache misses perform I/O under the pool mutex (simple, but serialises misses).
* Fixed-size key/value slots; no overflow pages for large values.
* Single-operation atomicity only; no multi-key transactions.

## Layout
```
include/butterdb.h   public API
src/pager.c          page I/O
src/bufpool.c        buffer pool + checkpoint journal
src/wal.c            write-ahead log + CRC-32
src/btree.c          B+tree + latch crabbing
src/db.c             open/recovery/checkpoint, public API
tools/cli.c          interactive shell
tools/bench.c        multi-threaded benchmark
tests/               test_basic, test_wal, test_concurrent
```
