/* bufpool.c — page cache with LRU eviction, pin counts and per-frame rwlock latches.
 *
 * Policy: NO-STEAL / FORCE-AT-CHECKPOINT. Dirty pages are never written to the
 * data file outside of a checkpoint, so between checkpoints the data file is
 * always exactly the last checkpointed state and WAL replay is sufficient for
 * recovery. Checkpoints themselves are made atomic with a double-write journal.
 *
 * Locking model:
 *   bp->lock       protects the frame table (which page lives in which frame,
 *                  pin counts, dirty bits, LRU clock). Held only briefly.
 *   frame->latch   protects the *contents* of a page. Acquired by the B-tree
 *                  layer after the frame is pinned; a pinned frame is never
 *                  evicted, so the latch can be held without bp->lock.
 */
#include "internal.h"
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

int bp_init(bufpool_t *bp, pager_t *pager, size_t nframes)
{
    bp->pager = pager;
    bp->nframes = nframes;
    bp->clock = 0;
    bp->hits = bp->misses = bp->evictions = 0;
    bp->frames = calloc(nframes, sizeof(frame_t));
    if (!bp->frames) return -1;
    bp->nbuckets = 1;
    while (bp->nbuckets < nframes * 2) bp->nbuckets <<= 1;
    bp->buckets = malloc(bp->nbuckets * sizeof(int));
    if (!bp->buckets) { free(bp->frames); return -1; }
    for (size_t i = 0; i < bp->nbuckets; i++) bp->buckets[i] = -1;
    for (size_t i = 0; i < nframes; i++) {
        bp->frames[i].hnext = -1;
        pthread_rwlock_init(&bp->frames[i].latch, NULL);
    }
    pthread_mutex_init(&bp->lock, NULL);
    return 0;
}

void bp_destroy(bufpool_t *bp)
{
    for (size_t i = 0; i < bp->nframes; i++)
        pthread_rwlock_destroy(&bp->frames[i].latch);
    free(bp->frames);
    free(bp->buckets);
    bp->frames = NULL;
    pthread_mutex_destroy(&bp->lock);
}

static inline size_t hslot(bufpool_t *bp, pgno_t pg) { return (pg * 2654435761u) & (bp->nbuckets - 1); }

static void hash_insert(bufpool_t *bp, frame_t *f)
{
    size_t h = hslot(bp, f->pgno);
    f->hnext = bp->buckets[h];
    bp->buckets[h] = (int)(f - bp->frames);
}
static void hash_remove(bufpool_t *bp, frame_t *f)
{
    int *link = &bp->buckets[hslot(bp, f->pgno)];
    int me = (int)(f - bp->frames);
    while (*link != -1 && *link != me) link = &bp->frames[*link].hnext;
    if (*link == me) *link = f->hnext;
    f->hnext = -1;
}
static frame_t *hash_find(bufpool_t *bp, pgno_t pg)
{
    for (int i = bp->buckets[hslot(bp, pg)]; i != -1; i = bp->frames[i].hnext)
        if (bp->frames[i].pgno == pg) return &bp->frames[i];
    return NULL;
}

frame_t *bp_fetch(bufpool_t *bp, pgno_t pgno)
{
    pthread_mutex_lock(&bp->lock);

    /* 1. cache hit: O(1) hash lookup */
    frame_t *f = hash_find(bp, pgno);
    if (f) {
        f->pin++;
        f->lru = ++bp->clock;
        bp->hits++;
        pthread_mutex_unlock(&bp->lock);
        return f;
    }
    bp->misses++;

    /* 2. choose a victim: least-recently-used frame that is clean and unpinned */
    frame_t *victim = NULL;
    for (size_t i = 0; i < bp->nframes; i++) {
        frame_t *c = &bp->frames[i];
        if (c->pin != 0 || c->dirty) continue;
        if (!c->valid) { victim = c; break; }
        if (!victim || c->lru < victim->lru) victim = c;
    }

    if (!victim) { /* every frame is pinned or dirty: caller must checkpoint */
        pthread_mutex_unlock(&bp->lock);
        return NULL;
    }

    /* 3. evict */
    if (victim->valid) { bp->evictions++; hash_remove(bp, victim); }

    /* 4. load. The frame is pinned before we release the table lock so no one
     * else can evict it; the I/O itself happens under bp->lock which keeps the
     * implementation simple at the cost of serialising cache misses. */
    victim->pgno = pgno;
    victim->valid = 1;
    victim->dirty = 0;
    victim->pin = 1;
    victim->lru = ++bp->clock;
    if (pager_read(bp->pager, pgno, victim->data) != 0) {
        victim->valid = 0;
        victim->pin = 0;
        pthread_mutex_unlock(&bp->lock);
        return NULL;
    }
    hash_insert(bp, victim);
    pthread_mutex_unlock(&bp->lock);
    return victim;
}

frame_t *bp_new(bufpool_t *bp, pgno_t *pgno_out)
{
    pgno_t pg = pager_alloc(bp->pager);
    if (pg == PGNO_NONE) return NULL;
    frame_t *f = bp_fetch(bp, pg);
    if (f) {
        memset(f->data, 0, PAGE_SIZE);
        *pgno_out = pg;
    }
    return f;
}

void bp_unpin(bufpool_t *bp, frame_t *f, int dirty)
{
    pthread_mutex_lock(&bp->lock);
    if (dirty && !f->dirty) { f->dirty = 1; __atomic_add_fetch(&bp->ndirty, 1, __ATOMIC_RELAXED); }
    assert(f->pin > 0);
    f->pin--;
    pthread_mutex_unlock(&bp->lock);
}


/* ---- double-write journal -------------------------------------------
 * layout: u32 magic | u32 count | count x (u32 pgno, PAGE_SIZE data) | u32 crc
 * crc covers everything before it. */
#define JOURNAL_MAGIC 0x434B5054u  /* "CKPT" */

static int write_full(int fd, const void *buf, size_t len)
{
    const uint8_t *p = buf;
    while (len) {
        ssize_t n = write(fd, p, len);
        if (n <= 0) return -1;
        p += n; len -= (size_t)n;
    }
    return 0;
}

int bp_checkpoint(bufpool_t *bp, const char *journal_path, const void *meta_page)
{
    pthread_mutex_lock(&bp->lock);
    int rc = -1;
    int jfd = open(journal_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (jfd < 0) goto out;

    /* 1. journal: meta page + every dirty page */
    uint32_t count = (uint32_t)__atomic_load_n(&bp->ndirty, __ATOMIC_RELAXED) + 1;
    uint32_t hdr[2] = { JOURNAL_MAGIC, count };
    uint32_t crc = bdb_crc32(hdr, sizeof hdr);
    if (write_full(jfd, hdr, sizeof hdr) != 0) goto out;
    for (size_t i = 0; i < bp->nframes; i++) {
        frame_t *f = &bp->frames[i];
        if (!(f->valid && f->dirty)) continue;
        uint32_t pg = f->pgno;
        if (write_full(jfd, &pg, 4) != 0 || write_full(jfd, f->data, PAGE_SIZE) != 0) goto out;
        crc = bdb_crc32_update(crc, &pg, 4);
        crc = bdb_crc32_update(crc, f->data, PAGE_SIZE);
    }
    uint32_t zero = 0;
    if (write_full(jfd, &zero, 4) != 0 || write_full(jfd, meta_page, PAGE_SIZE) != 0) goto out;
    crc = bdb_crc32_update(crc, &zero, 4);
    crc = bdb_crc32_update(crc, meta_page, PAGE_SIZE);
    if (write_full(jfd, &crc, 4) != 0 || fsync(jfd) != 0) goto out;

    /* 2. data file: pages first, meta last */
    for (size_t i = 0; i < bp->nframes; i++) {
        frame_t *f = &bp->frames[i];
        if (!(f->valid && f->dirty)) continue;
        if (pager_write(bp->pager, f->pgno, f->data) != 0) goto out;
        f->dirty = 0;
        __atomic_sub_fetch(&bp->ndirty, 1, __ATOMIC_RELAXED);
    }
    if (pager_sync(bp->pager) != 0) goto out;
    if (pager_write(bp->pager, 0, meta_page) != 0 || pager_sync(bp->pager) != 0) goto out;

    /* 3. journal no longer needed */
    close(jfd); jfd = -1;
    unlink(journal_path);
    rc = 0;
out:
    if (jfd >= 0) close(jfd);
    pthread_mutex_unlock(&bp->lock);
    return rc;
}

int bp_journal_recover(pager_t *pager, const char *journal_path)
{
    int fd = open(journal_path, O_RDONLY);
    if (fd < 0) return 0;                       /* no journal: nothing to do */
    struct stat st;
    if (fstat(fd, &st) != 0) { close(fd); return -1; }
    size_t size = (size_t)st.st_size;
    uint8_t *buf = malloc(size);
    int rc = 0;
    if (!buf || pread(fd, buf, size, 0) != (ssize_t)size) { rc = -1; goto done; }

    uint32_t hdr[2];
    if (size < 12) goto discard;
    memcpy(hdr, buf, 8);
    size_t need = 8 + (size_t)hdr[1] * (4 + PAGE_SIZE) + 4;
    if (hdr[0] != JOURNAL_MAGIC || need != size) goto discard;
    uint32_t crc; memcpy(&crc, buf + size - 4, 4);
    if (bdb_crc32(buf, size - 4) != crc) goto discard;

    /* complete journal: re-apply every page (idempotent) */
    for (uint32_t i = 0; i < hdr[1]; i++) {
        uint8_t *e = buf + 8 + (size_t)i * (4 + PAGE_SIZE);
        uint32_t pg; memcpy(&pg, e, 4);
        if (pg >= pager->npages) pager->npages = pg + 1;
        if (pager_write(pager, pg, e + 4) != 0) { rc = -1; goto done; }
    }
    if (pager_sync(pager) != 0) rc = -1;
discard:
    /* torn journal => the data file was never touched; either way drop it */
    close(fd); fd = -1;
    unlink(journal_path);
done:
    if (fd >= 0) close(fd);
    free(buf);
    return rc;
}
