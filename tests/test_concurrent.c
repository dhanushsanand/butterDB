/* Many writers + readers + scanners hammering the tree at once; verify final state. */
#include "butterdb.h"
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DB "test_concurrent.db"
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #x); exit(1); } } while (0)
#define WRITERS 8
#define READERS 4
#define PER_WRITER 10000

static butterdb_t *db;
static atomic_int done = 0;

static void *writer(void *arg)
{
    long id = (long)arg; char k[32], v[64];
    for (int i = 0; i < PER_WRITER; i++) {
        int j = (int)(((long)i * 7919) % PER_WRITER);
        snprintf(k, sizeof k, "w%ld-%06d", id, j); snprintf(v, sizeof v, "val-%ld-%d", id, j);
        CHECK(butterdb_put(db, k, strlen(k), v, strlen(v)) == BDB_OK);
        if (j % 10 == 0) CHECK(butterdb_delete(db, k, strlen(k)) == BDB_OK);   /* churn */
    }
    return NULL;
}
static int noop_cb(const char *k, size_t kl, const void *v, size_t vl, void *ud)
{ (void)k; (void)kl; (void)v; (void)vl; (void)ud; return 0; }
static void *reader(void *arg)
{
    long id = (long)arg; char k[32], out[128]; size_t vl; unsigned long reads = 0;
    while (!atomic_load(&done)) {
        snprintf(k, sizeof k, "w%ld-%06d", id % WRITERS, (int)(reads % PER_WRITER)); vl = sizeof out;
        int rc = butterdb_get(db, k, strlen(k), out, &vl);
        CHECK(rc == BDB_OK || rc == BDB_NOTFOUND);
        if (reads % 500 == 0) CHECK(butterdb_scan(db, k, strlen(k), noop_cb, NULL) == BDB_OK);
        reads++;
    }
    return NULL;
}

int main(void)
{
    remove(DB); remove(DB ".wal");
    bdb_options_t o = { .pool_frames = 128, .sync_wal = 0 };
    CHECK(butterdb_open(&db, DB, &o) == BDB_OK);
    pthread_t w[WRITERS], r[READERS];
    for (long i = 0; i < READERS; i++) pthread_create(&r[i], NULL, reader, (void *)i);
    for (long i = 0; i < WRITERS; i++) pthread_create(&w[i], NULL, writer, (void *)i);
    for (int i = 0; i < WRITERS; i++) pthread_join(w[i], NULL);
    atomic_store(&done, 1);
    for (int i = 0; i < READERS; i++) pthread_join(r[i], NULL);

    char k[32], v[64], out[128]; size_t vl;
    for (long id = 0; id < WRITERS; id++)
        for (int j = 0; j < PER_WRITER; j++) {
            snprintf(k, sizeof k, "w%ld-%06d", id, j); snprintf(v, sizeof v, "val-%ld-%d", id, j); vl = sizeof out;
            int rc = butterdb_get(db, k, strlen(k), out, &vl);
            if (j % 10 == 0) CHECK(rc == BDB_NOTFOUND);
            else { CHECK(rc == BDB_OK); CHECK(vl == strlen(v) && !memcmp(out, v, vl)); }
        }
    bdb_stats_t s; butterdb_stats(db, &s);
    printf("  height=%u pages=%u splits=%llu\n", s.tree_height, s.pages, (unsigned long long)s.splits);
    butterdb_close(db);
    remove(DB); remove(DB ".wal");
    puts("test_concurrent: PASS");
    return 0;
}
