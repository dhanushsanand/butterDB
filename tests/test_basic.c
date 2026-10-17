/* Single-threaded correctness: CRUD, ordering, persistence, splits, eviction. */
#include "butterdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DB "test_basic.db"
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #x); exit(1); } } while (0)

static char prev_key[64];
static int order_cb(const char *k, size_t kl, const void *v, size_t vl, void *ud)
{
    (void)v; (void)vl; (void)ud;
    char cur[64]; snprintf(cur, sizeof cur, "%.*s", (int)kl, k);
    if (prev_key[0]) CHECK(strcmp(prev_key, cur) < 0);   /* strictly increasing */
    strcpy(prev_key, cur);
    return 0;
}
static int tally_cb(const char *k, size_t kl, const void *v, size_t vl, void *ud)
{ (void)k; (void)kl; (void)v; (void)vl; (*(int *)ud)++; return 0; }

int main(void)
{
    remove(DB); remove(DB ".wal");
    butterdb_t *db;
    bdb_options_t o = { .pool_frames = 64, .sync_wal = 0 };  /* tiny pool forces eviction */
    CHECK(butterdb_open(&db, DB, &o) == BDB_OK);

    char k[32], v[64], out[128]; size_t vl;
    const int N = 20000;

    /* scrambled insert order so splits happen all over the tree */
    for (int i = 0; i < N; i++) {
        int j = (int)(((long)i * 7919) % N);
        snprintf(k, sizeof k, "k%08d", j); snprintf(v, sizeof v, "v%d", j);
        CHECK(butterdb_put(db, k, strlen(k), v, strlen(v)) == BDB_OK);
    }
    for (int i = 0; i < N; i++) {
        snprintf(k, sizeof k, "k%08d", i); snprintf(v, sizeof v, "v%d", i); vl = sizeof out;
        CHECK(butterdb_get(db, k, strlen(k), out, &vl) == BDB_OK);
        CHECK(vl == strlen(v) && memcmp(out, v, vl) == 0);
    }
    /* overwrite in place */
    CHECK(butterdb_put(db, "k00000005", 9, "updated", 7) == BDB_OK);
    vl = sizeof out;
    CHECK(butterdb_get(db, "k00000005", 9, out, &vl) == BDB_OK && vl == 7 && !memcmp(out, "updated", 7));
    /* delete every even key */
    for (int i = 0; i < N; i += 2) { snprintf(k, sizeof k, "k%08d", i); CHECK(butterdb_delete(db, k, strlen(k)) == BDB_OK); }
    CHECK(butterdb_delete(db, "k00000000", 9) == BDB_NOTFOUND);
    /* ordered full scan, counted scan, ranged scan */
    prev_key[0] = 0; CHECK(butterdb_scan(db, NULL, 0, order_cb, NULL) == BDB_OK);
    int n = 0; CHECK(butterdb_scan(db, NULL, 0, tally_cb, &n) == BDB_OK); CHECK(n == N / 2);
    n = 0; CHECK(butterdb_scan(db, "k00019990", 9, tally_cb, &n) == BDB_OK); CHECK(n == 5);
    /* argument validation */
    CHECK(butterdb_put(db, "", 0, "x", 1) == BDB_EINVAL);
    CHECK(butterdb_put(db, "toolongtoolongtoolongtoolongtoolong", 35, "x", 1) == BDB_EINVAL);
    CHECK(butterdb_put(db, "big", 3, out, BDB_MAX_VAL + 1) == BDB_EINVAL);
    CHECK(butterdb_put(db, "big", 3, out, BDB_MAX_VAL) == BDB_OK);

    bdb_stats_t s; butterdb_stats(db, &s);
    printf("  height=%u pages=%u splits=%llu evictions=%llu\n", s.tree_height, s.pages,
           (unsigned long long)s.splits, (unsigned long long)s.pool_evictions);
    CHECK(s.tree_height >= 3 && s.splits > 0 && s.pool_evictions > 0);
    butterdb_close(db);

    /* reopen: everything must persist */
    CHECK(butterdb_open(&db, DB, &o) == BDB_OK);
    for (int i = 0; i < N; i++) {
        snprintf(k, sizeof k, "k%08d", i); vl = sizeof out;
        int rc = butterdb_get(db, k, strlen(k), out, &vl);
        CHECK(rc == (i % 2 ? BDB_OK : BDB_NOTFOUND));
    }
    butterdb_close(db);
    remove(DB); remove(DB ".wal");
    puts("test_basic: PASS");
    return 0;
}
