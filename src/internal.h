#ifndef BDB_INTERNAL_H
#define BDB_INTERNAL_H

#include <pthread.h>
#include <stdint.h>
#include <stddef.h>
#include <assert.h>
#include "butterdb.h"

#define PAGE_SIZE 4096
typedef uint32_t pgno_t;
#define PGNO_NONE 0u   /* page 0 is the meta page; never a tree node */

/* ------------------------------------------------------------------ pager */
typedef struct {
    int             fd;
    pgno_t          npages;
    pthread_mutex_t lock;
} pager_t;

int    pager_open(pager_t *p, const char *path);
void   pager_close(pager_t *p);
int    pager_read(pager_t *p, pgno_t pgno, void *buf);
int    pager_write(pager_t *p, pgno_t pgno, const void *buf);
pgno_t pager_alloc(pager_t *p);          /* returns PGNO_NONE on failure */
int    pager_sync(pager_t *p);

/* ------------------------------------------------------------ buffer pool */
typedef struct {
    pgno_t           pgno;
    int              valid;
    int              dirty;
    int              pin;
    uint64_t         lru;
    int              hnext;     /* next frame in hash chain, -1 = end */
    pthread_rwlock_t latch;     /* page latch: readers / one writer */
    uint8_t          data[PAGE_SIZE];
} frame_t;

typedef struct {
    pager_t        *pager;
    frame_t        *frames;
    size_t          nframes;
    int            *buckets;    /* pgno -> frame index hash (chained) */
    size_t          nbuckets;
    uint64_t        clock;
    pthread_mutex_t lock;       /* protects the frame table (not page contents) */
    size_t          ndirty;
    uint64_t        hits, misses, evictions;
} bufpool_t;

#define BDB_EBUSY (-4)   /* internal: pool has no clean frame — checkpoint and retry */

int      bp_init(bufpool_t *bp, pager_t *pager, size_t nframes);
void     bp_destroy(bufpool_t *bp);
frame_t *bp_fetch(bufpool_t *bp, pgno_t pgno);        /* pins; NULL if pool exhausted */
frame_t *bp_new(bufpool_t *bp, pgno_t *pgno_out);     /* alloc + fetch + pin */
void     bp_unpin(bufpool_t *bp, frame_t *f, int dirty);
/* Atomic checkpoint: dirty pages + meta page are first written to the
 * double-write journal (fsync), then to the data file (fsync), then the
 * journal is removed. A crash anywhere leaves a recoverable state. */
int      bp_checkpoint(bufpool_t *bp, const char *journal_path, const void *meta_page);
int      bp_journal_recover(pager_t *pager, const char *journal_path);

static inline void bp_rdlatch(frame_t *f) { pthread_rwlock_rdlock(&f->latch); }
static inline void bp_wrlatch(frame_t *f) { pthread_rwlock_wrlock(&f->latch); }
static inline void bp_unlatch(frame_t *f) { pthread_rwlock_unlock(&f->latch); }

/* --------------------------------------------------------------------- WAL */
enum { WAL_PUT = 1, WAL_DEL = 2 };

typedef struct {
    int             fd;
    int             sync;
    pthread_mutex_t lock;
    uint64_t        records, bytes;
} wal_t;

typedef int (*wal_replay_cb)(uint8_t op, const char *key, size_t klen,
                             const void *val, size_t vlen, void *ud);

int  wal_open(wal_t *w, const char *path, int sync);
void wal_close(wal_t *w);
int  wal_append(wal_t *w, uint8_t op, const char *key, size_t klen,
                const void *val, size_t vlen);
int  wal_replay(wal_t *w, wal_replay_cb cb, void *ud, size_t *nreplayed);
int  wal_reset(wal_t *w);

uint32_t bdb_crc32(const void *buf, size_t len);
uint32_t bdb_crc32_update(uint32_t crc, const void *buf, size_t len);

/* ------------------------------------------------------------------ btree */
#define KEY_SIZE  32
#define VAL_SLOT  128       /* 2-byte length + up to 126 bytes */
#define NODE_LEAF 1
#define NODE_INTERNAL 2

typedef struct {
    uint8_t  type;
    uint8_t  pad;
    uint16_t nkeys;
    pgno_t   next;       /* leaf: right sibling */
    uint32_t reserved;
} node_hdr_t;            /* 12 bytes */

typedef struct {
    bufpool_t       *bp;
    pgno_t           root;
    pthread_rwlock_t root_latch;   /* protects `root` and acts as the virtual parent of the root */
    uint64_t         splits;
} btree_t;

int  btree_init_empty(btree_t *t, bufpool_t *bp, pgno_t *root_out);
int  btree_attach(btree_t *t, bufpool_t *bp, pgno_t root);
void btree_destroy(btree_t *t);
int  btree_get(btree_t *t, const char *key, size_t klen, void *val, size_t *vlen);
int  btree_put(btree_t *t, const char *key, size_t klen, const void *val, size_t vlen);
int  btree_delete(btree_t *t, const char *key, size_t klen);
int  btree_scan(btree_t *t, const char *start, size_t slen, bdb_scan_cb cb, void *ud);
uint32_t btree_height(btree_t *t);

#endif
