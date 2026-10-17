/*
 * ButterDB — a multi-threaded, persistent key-value store built on a B+tree.
 *
 * Public API. Everything here is thread-safe: any number of threads may call
 * put/get/delete/scan concurrently on the same handle.
 */
#ifndef BUTTERDB_H
#define BUTTERDB_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define BDB_OK        0
#define BDB_NOTFOUND  1
#define BDB_ERR      (-1)
#define BDB_EINVAL   (-2)
#define BDB_ECORRUPT (-3)

/* Keys are NUL-free byte strings up to BDB_MAX_KEY bytes.
 * Values are arbitrary bytes up to BDB_MAX_VAL bytes. */
#define BDB_MAX_KEY 32
#define BDB_MAX_VAL 126

typedef struct butterdb butterdb_t;

typedef struct {
    size_t pool_frames;   /* buffer-pool frames (pages cached in RAM). 0 = default (256) */
    int    sync_wal;      /* 1 = fsync the WAL on every write (durable); 0 = OS-buffered */
} bdb_options_t;

typedef struct {
    uint64_t puts, gets, hits, dels, splits;
    uint64_t pool_hits, pool_misses, pool_evictions;
    uint64_t wal_records, wal_bytes;
    uint32_t pages;        /* pages in the data file */
    uint32_t tree_height;
} bdb_stats_t;

/* Return non-zero from the callback to stop the scan early. */
typedef int (*bdb_scan_cb)(const char *key, size_t klen,
                           const void *val, size_t vlen, void *ud);

int  butterdb_open(butterdb_t **out, const char *path, const bdb_options_t *opts);
void butterdb_close(butterdb_t *db);

int  butterdb_put(butterdb_t *db, const char *key, size_t klen,
                  const void *val, size_t vlen);
int  butterdb_get(butterdb_t *db, const char *key, size_t klen,
                  void *val_out, size_t *vlen_inout);
int  butterdb_delete(butterdb_t *db, const char *key, size_t klen);

/* Ordered scan starting at the first key >= start (NULL start = beginning). */
int  butterdb_scan(butterdb_t *db, const char *start, size_t slen,
                   bdb_scan_cb cb, void *ud);

/* Flush dirty pages, fsync, truncate the WAL. */
int  butterdb_checkpoint(butterdb_t *db);

void butterdb_stats(butterdb_t *db, bdb_stats_t *out);

/* Test hook: free the handle WITHOUT flushing anything, simulating a crash.
 * The next open() must recover committed writes from the WAL. */
void butterdb_crash(butterdb_t *db);

const char *butterdb_strerror(int rc);

#ifdef __cplusplus
}
#endif
#endif
