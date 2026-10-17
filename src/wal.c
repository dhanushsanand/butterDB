/* wal.c — logical write-ahead log.
 *
 * Record layout (little-endian, on disk):
 *   u32 crc      CRC-32 over everything after this field
 *   u16 len      total record length including this header
 *   u8  op       WAL_PUT | WAL_DEL
 *   u8  klen
 *   u16 vlen
 *   u16 pad
 *   key[klen] val[vlen]
 *
 * Every mutating operation is appended (and optionally fsync'd) *before* the
 * B-tree is modified. Replay is idempotent, so on recovery we simply re-apply
 * every intact record since the last checkpoint.
 */
#include "internal.h"
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

typedef struct {
    uint32_t crc;
    uint16_t len;
    uint8_t  op;
    uint8_t  klen;
    uint16_t vlen;
    uint16_t pad;
} wal_hdr_t;

#define WAL_HDR 12
_Static_assert(sizeof(wal_hdr_t) == WAL_HDR, "wal header size");

static uint32_t crc_table[256];
static pthread_once_t crc_once = PTHREAD_ONCE_INIT;

static void crc_init(void)
{
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int k = 0; k < 8; k++) c = (c & 1) ? 0xEDB88320u ^ (c >> 1) : c >> 1;
        crc_table[i] = c;
    }
}

uint32_t bdb_crc32_update(uint32_t crc, const void *buf, size_t len)
{
    pthread_once(&crc_once, crc_init);
    const uint8_t *p = buf;
    uint32_t c = crc ^ 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++) c = crc_table[(c ^ p[i]) & 0xFF] ^ (c >> 8);
    return c ^ 0xFFFFFFFFu;
}
uint32_t bdb_crc32(const void *buf, size_t len) { return bdb_crc32_update(0, buf, len); }

int wal_open(wal_t *w, const char *path, int sync)
{
    w->fd = open(path, O_RDWR | O_CREAT | O_APPEND, 0644);
    if (w->fd < 0) return -1;
    w->sync = sync;
    w->records = w->bytes = 0;
    pthread_mutex_init(&w->lock, NULL);
    return 0;
}

void wal_close(wal_t *w)
{
    if (w->fd >= 0) close(w->fd);
    w->fd = -1;
    pthread_mutex_destroy(&w->lock);
}

int wal_append(wal_t *w, uint8_t op, const char *key, size_t klen,
               const void *val, size_t vlen)
{
    uint8_t buf[WAL_HDR + BDB_MAX_KEY + BDB_MAX_VAL];
    size_t len = WAL_HDR + klen + vlen;
    wal_hdr_t *h = (wal_hdr_t *)buf;
    h->len = (uint16_t)len;
    h->op = op;
    h->klen = (uint8_t)klen;
    h->vlen = (uint16_t)vlen;
    h->pad = 0;
    memcpy(buf + WAL_HDR, key, klen);
    if (vlen) memcpy(buf + WAL_HDR + klen, val, vlen);
    h->crc = bdb_crc32(buf + 4, len - 4);

    pthread_mutex_lock(&w->lock);
    ssize_t n = write(w->fd, buf, len);
    int rc = (n == (ssize_t)len) ? 0 : -1;
    if (rc == 0 && w->sync && fsync(w->fd) != 0) rc = -1;
    if (rc == 0) { w->records++; w->bytes += len; }
    pthread_mutex_unlock(&w->lock);
    return rc;
}

int wal_replay(wal_t *w, wal_replay_cb cb, void *ud, size_t *nreplayed)
{
    struct stat st;
    if (fstat(w->fd, &st) != 0) return -1;
    size_t size = (size_t)st.st_size;
    *nreplayed = 0;
    if (size == 0) return 0;

    uint8_t *data = malloc(size);
    if (!data) return -1;
    if (pread(w->fd, data, size, 0) != (ssize_t)size) { free(data); return -1; }

    size_t off = 0, good = 0;
    while (off + WAL_HDR <= size) {
        wal_hdr_t h;
        memcpy(&h, data + off, WAL_HDR);
        if (h.len < WAL_HDR || off + h.len > size) break;               /* torn tail */
        if (bdb_crc32(data + off + 4, h.len - 4) != h.crc) break;        /* corrupt */
        if ((size_t)h.klen + h.vlen + WAL_HDR != h.len) break;
        const char *key = (const char *)(data + off + WAL_HDR);
        const void *val = data + off + WAL_HDR + h.klen;
        if (cb(h.op, key, h.klen, val, h.vlen, ud) != 0) { free(data); return -1; }
        (*nreplayed)++;
        off += h.len;
        good = off;
    }
    free(data);
    /* drop a torn/corrupt tail so it is never replayed (re-stat: the callback
     * may have checkpointed and truncated the log in the meantime) */
    if (fstat(w->fd, &st) == 0 && (size_t)st.st_size > good) {
        if (ftruncate(w->fd, (off_t)good) != 0) return -1;
    }
    return 0;
}

int wal_reset(wal_t *w)
{
    pthread_mutex_lock(&w->lock);
    int rc = ftruncate(w->fd, 0) == 0 ? 0 : -1;
    if (rc == 0) rc = fsync(w->fd) == 0 ? 0 : -1;
    pthread_mutex_unlock(&w->lock);
    return rc;
}
