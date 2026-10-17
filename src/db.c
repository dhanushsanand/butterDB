/* db.c — ties pager + buffer pool + WAL + B+tree together.
 *
 * Files:  <path>       data file (page 0 = meta page, 1.. = tree nodes)
 *         <path>.wal   write-ahead log
 *
 * Write path:  WAL append (+fsync)  ->  B-tree update in buffer pool
 * Checkpoint:  flush dirty pages -> fsync data -> write meta -> fsync -> truncate WAL
 * Recovery:    open data file at last checkpoint -> replay WAL -> checkpoint
 */
#include "internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BDB_MAGIC   0x42555454u   /* "BUTT" */
#define BDB_VERSION 1
#define DEFAULT_FRAMES 256

typedef struct {
    uint32_t magic;
    uint32_t version;
    pgno_t   root;
    pgno_t   npages;
} meta_t;

struct butterdb {
    pager_t          pager;
    bufpool_t        bp;
    wal_t            wal;
    btree_t          tree;
    meta_t           meta;
    pthread_rwlock_t ckpt_lock;   /* writers: shared; checkpoint: exclusive */
    uint64_t         puts, gets, hits, dels;
    char             jpath[4096];
};

const char *butterdb_strerror(int rc)
{
    switch (rc) {
    case BDB_OK:       return "ok";
    case BDB_NOTFOUND: return "not found";
    case BDB_ERR:      return "I/O or resource error";
    case BDB_EINVAL:   return "invalid argument";
    case BDB_ECORRUPT: return "database corrupt";
    default:           return "unknown";
    }
}

static void journal_path(const char *path, char *out, size_t n) { snprintf(out, n, "%s.ckpt", path); }

typedef struct { const char *key; size_t klen; const void *val; size_t vlen; void *out; size_t *vlen_io; bdb_scan_cb cb; void *ud; } opargs_t;
typedef int (*tree_op_t)(butterdb_t *db, const opargs_t *a);

/* Run a tree operation; if the pool has no clean frame left (BDB_EBUSY),
 * checkpoint — which cleans every frame — and try again. */
static int with_pool_retry(butterdb_t *db, tree_op_t op, const opargs_t *a)
{
    int rc = BDB_EBUSY;
    for (int tries = 0; tries < 4; tries++) {
        rc = op(db, a);
        if (rc != BDB_EBUSY) return rc;
        if (butterdb_checkpoint(db) != BDB_OK) return BDB_ERR;
    }
    return BDB_ERR;
}
static int op_get(butterdb_t *db, const opargs_t *a)    { return btree_get(&db->tree, a->key, a->klen, a->out, a->vlen_io); }
static int op_scan(butterdb_t *db, const opargs_t *a)   { return btree_scan(&db->tree, a->key, a->klen, a->cb, a->ud); }
static int op_rput(butterdb_t *db, const opargs_t *a)   { return btree_put(&db->tree, a->key, a->klen, a->val, a->vlen); }
static int op_rdel(butterdb_t *db, const opargs_t *a)   { return btree_delete(&db->tree, a->key, a->klen); }
static int op_put(butterdb_t *db, const opargs_t *a);
static int op_del(butterdb_t *db, const opargs_t *a);

static int replay_cb(uint8_t op, const char *key, size_t klen,
                     const void *val, size_t vlen, void *ud)
{
    butterdb_t *db = ud;
    opargs_t a = { .key = key, .klen = klen, .val = val, .vlen = vlen };
    if (op == WAL_PUT) return with_pool_retry(db, op_rput, &a) == BDB_OK ? 0 : -1;
    if (op == WAL_DEL) return with_pool_retry(db, op_rdel, &a) < 0 ? -1 : 0;
    return -1;
}

int butterdb_open(butterdb_t **out, const char *path, const bdb_options_t *opts)
{
    if (!out || !path) return BDB_EINVAL;
    size_t frames = (opts && opts->pool_frames) ? opts->pool_frames : DEFAULT_FRAMES;
    if (frames < 8) frames = 8;
    int sync = opts ? opts->sync_wal : 1;

    butterdb_t *db = calloc(1, sizeof *db);
    if (!db) return BDB_ERR;
    pthread_rwlock_init(&db->ckpt_lock, NULL);

    if (pager_open(&db->pager, path) != 0) { free(db); return BDB_ERR; }
    /* finish (or discard) an interrupted checkpoint before reading the meta page */
    db->jpath[0] = 0; journal_path(path, db->jpath, sizeof db->jpath);
    if (bp_journal_recover(&db->pager, db->jpath) != 0) { pager_close(&db->pager); free(db); return BDB_ERR; }
    if (bp_init(&db->bp, &db->pager, frames) != 0) { pager_close(&db->pager); free(db); return BDB_ERR; }

    int fresh = 0;
    if (db->pager.npages == 0) {
        fresh = 1;
        uint8_t zero[PAGE_SIZE] = {0};                 /* reserve meta page 0 */
        if (pager_write(&db->pager, 0, zero) != 0) goto fail;
        db->pager.npages = 1;
        pgno_t root;
        if (btree_init_empty(&db->tree, &db->bp, &root) != 0) goto fail;
        db->meta.magic = BDB_MAGIC;
        db->meta.version = BDB_VERSION;
    } else {
        uint8_t page[PAGE_SIZE];
        if (pager_read(&db->pager, 0, page) != 0) goto fail;
        memcpy(&db->meta, page, sizeof db->meta);
        if (db->meta.magic != BDB_MAGIC || db->meta.version != BDB_VERSION) {
            bp_destroy(&db->bp); pager_close(&db->pager); free(db);
            return BDB_ECORRUPT;
        }
        /* pages allocated after the last checkpoint are garbage: forget them */
        db->pager.npages = db->meta.npages;
        btree_attach(&db->tree, &db->bp, db->meta.root);
    }

    char walpath[4096];
    snprintf(walpath, sizeof walpath, "%s.wal", path);
    if (wal_open(&db->wal, walpath, sync) != 0) goto fail;

    size_t replayed = 0;
    if (!fresh && wal_replay(&db->wal, replay_cb, db, &replayed) != 0) goto fail;

    /* a checkpoint after a fresh init or a recovery makes the on-disk state consistent */
    if (fresh || replayed > 0) {
        if (butterdb_checkpoint(db) != BDB_OK) goto fail;
    }
    *out = db;
    return BDB_OK;

fail:
    if (db->wal.fd > 0) wal_close(&db->wal);
    bp_destroy(&db->bp);
    pager_close(&db->pager);
    free(db);
    return BDB_ERR;
}

int butterdb_checkpoint(butterdb_t *db)
{
    pthread_rwlock_wrlock(&db->ckpt_lock);
    uint8_t meta_page[PAGE_SIZE] = {0};
    db->meta.root = db->tree.root;
    db->meta.npages = db->pager.npages;
    memcpy(meta_page, &db->meta, sizeof db->meta);
    int rc = BDB_OK;
    if (bp_checkpoint(&db->bp, db->jpath, meta_page) != 0 || wal_reset(&db->wal) != 0)
        rc = BDB_ERR;
    pthread_rwlock_unlock(&db->ckpt_lock);
    return rc;
}

void butterdb_close(butterdb_t *db)
{
    if (!db) return;
    butterdb_checkpoint(db);
    butterdb_crash(db);
}

void butterdb_crash(butterdb_t *db)
{
    if (!db) return;
    wal_close(&db->wal);
    btree_destroy(&db->tree);
    bp_destroy(&db->bp);
    pager_close(&db->pager);
    pthread_rwlock_destroy(&db->ckpt_lock);
    free(db);
}

/* WAL first, then the tree — both under the shared checkpoint lock so a
 * checkpoint can never slip between the log record and the page update. */
static int op_put(butterdb_t *db, const opargs_t *a)
{
    pthread_rwlock_rdlock(&db->ckpt_lock);
    int rc = BDB_ERR;
    if (wal_append(&db->wal, WAL_PUT, a->key, a->klen, a->val, a->vlen) == 0)
        rc = btree_put(&db->tree, a->key, a->klen, a->val, a->vlen);
    pthread_rwlock_unlock(&db->ckpt_lock);
    return rc;
}
static int op_del(butterdb_t *db, const opargs_t *a)
{
    pthread_rwlock_rdlock(&db->ckpt_lock);
    int rc = BDB_ERR;
    if (wal_append(&db->wal, WAL_DEL, a->key, a->klen, NULL, 0) == 0)
        rc = btree_delete(&db->tree, a->key, a->klen);
    pthread_rwlock_unlock(&db->ckpt_lock);
    return rc;
}
/* Proactive checkpoint once most of the pool is dirty, so writers rarely hit
 * the EBUSY path. Non-blocking: if another checkpoint is running, skip. */
static void maybe_checkpoint(butterdb_t *db)
{
    if (__atomic_load_n(&db->bp.ndirty, __ATOMIC_RELAXED) * 4 < db->bp.nframes * 3) return;  /* unlocked heuristic read */
    if (pthread_rwlock_trywrlock(&db->ckpt_lock) != 0) return;
    pthread_rwlock_unlock(&db->ckpt_lock);
    butterdb_checkpoint(db);
}

static int check_key(const char *key, size_t klen)
{
    if (!key || klen == 0 || klen > BDB_MAX_KEY) return 0;
    return memchr(key, 0, klen) == NULL;
}

int butterdb_put(butterdb_t *db, const char *key, size_t klen, const void *val, size_t vlen)
{
    if (!db || !check_key(key, klen) || vlen > BDB_MAX_VAL || (vlen && !val)) return BDB_EINVAL;
    opargs_t a = { .key = key, .klen = klen, .val = val, .vlen = vlen };
    int rc = with_pool_retry(db, op_put, &a);
    __atomic_add_fetch(&db->puts, 1, __ATOMIC_RELAXED);
    maybe_checkpoint(db);
    return rc;
}

int butterdb_get(butterdb_t *db, const char *key, size_t klen, void *val_out, size_t *vlen)
{
    if (!db || !check_key(key, klen)) return BDB_EINVAL;
    opargs_t a = { .key = key, .klen = klen, .out = val_out, .vlen_io = vlen };
    int rc = with_pool_retry(db, op_get, &a);
    __atomic_add_fetch(&db->gets, 1, __ATOMIC_RELAXED);
    if (rc == BDB_OK) __atomic_add_fetch(&db->hits, 1, __ATOMIC_RELAXED);
    return rc;
}

int butterdb_delete(butterdb_t *db, const char *key, size_t klen)
{
    if (!db || !check_key(key, klen)) return BDB_EINVAL;
    opargs_t a = { .key = key, .klen = klen };
    int rc = with_pool_retry(db, op_del, &a);
    __atomic_add_fetch(&db->dels, 1, __ATOMIC_RELAXED);
    maybe_checkpoint(db);
    return rc;
}

int butterdb_scan(butterdb_t *db, const char *start, size_t slen, bdb_scan_cb cb, void *ud)
{
    if (!db || !cb) return BDB_EINVAL;
    if (start && slen > BDB_MAX_KEY) return BDB_EINVAL;
    opargs_t a = { .key = start, .klen = start ? slen : 0, .cb = cb, .ud = ud };
    return with_pool_retry(db, op_scan, &a);
}

void butterdb_stats(butterdb_t *db, bdb_stats_t *s)
{
    memset(s, 0, sizeof *s);
    if (!db) return;
    s->puts = db->puts; s->gets = db->gets; s->hits = db->hits; s->dels = db->dels;
    s->splits = db->tree.splits;
    s->pool_hits = db->bp.hits; s->pool_misses = db->bp.misses; s->pool_evictions = db->bp.evictions;
    s->wal_records = db->wal.records; s->wal_bytes = db->wal.bytes;
    s->pages = db->pager.npages;
    s->tree_height = btree_height(&db->tree);
}
