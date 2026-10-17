/* bench.c — multi-threaded throughput benchmark.
 *   bench <file> [n_keys] [threads] [sync_wal 0|1]
 */
#include "butterdb.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static butterdb_t *db;
static int N, T;

static double now(void) { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return ts.tv_sec + ts.tv_nsec / 1e9; }

static void *writer(void *arg)
{
    long id = (long)arg; char k[32], v[64];
    for (int i = (int)id; i < N; i += T) {
        snprintf(k, sizeof k, "key%010d", i);
        snprintf(v, sizeof v, "value-%d-%ld", i, id);
        if (butterdb_put(db, k, strlen(k), v, strlen(v)) != BDB_OK) { fprintf(stderr, "put failed\n"); exit(1); }
    }
    return NULL;
}
static void *reader(void *arg)
{
    long id = (long)arg; char k[32], buf[128]; size_t vl; long miss = 0;
    for (int i = (int)id; i < N; i += T) {
        snprintf(k, sizeof k, "key%010d", i); vl = sizeof buf;
        if (butterdb_get(db, k, strlen(k), buf, &vl) != BDB_OK) miss++;
    }
    return (void *)miss;
}

int main(int argc, char **argv)
{
    if (argc < 2) { fprintf(stderr, "usage: %s <file> [n_keys] [threads] [sync_wal]\n", argv[0]); return 1; }
    N = argc > 2 ? atoi(argv[2]) : 100000;
    T = argc > 3 ? atoi(argv[3]) : 4;
    if (T > 64) T = 64;
    bdb_options_t o = { .pool_frames = 4096, .sync_wal = argc > 4 ? atoi(argv[4]) : 0 };
    remove(argv[1]); char w[600]; snprintf(w, sizeof w, "%s.wal", argv[1]); remove(w);
    if (butterdb_open(&db, argv[1], &o) != BDB_OK) { fprintf(stderr, "open failed\n"); return 1; }

    pthread_t th[64];
    double t0 = now();
    for (long i = 0; i < T; i++) pthread_create(&th[i], NULL, writer, (void *)i);
    for (int i = 0; i < T; i++) pthread_join(th[i], NULL);
    double t1 = now();
    long miss = 0;
    for (long i = 0; i < T; i++) pthread_create(&th[i], NULL, reader, (void *)i);
    for (int i = 0; i < T; i++) { void *r; pthread_join(th[i], &r); miss += (long)r; }
    double t2 = now();
    double t3 = now(); butterdb_checkpoint(db); double t4 = now();

    bdb_stats_t s; butterdb_stats(db, &s);
    printf("keys=%d threads=%d sync_wal=%d\n", N, T, o.sync_wal);
    printf("put:  %.2fs  %.0f ops/s\n", t1 - t0, N / (t1 - t0));
    printf("get:  %.2fs  %.0f ops/s  (misses=%ld)\n", t2 - t1, N / (t2 - t1), miss);
    printf("checkpoint: %.3fs  pages=%u height=%u splits=%llu pool_miss=%llu\n",
           t4 - t3, s.pages, s.tree_height, (unsigned long long)s.splits, (unsigned long long)s.pool_misses);
    butterdb_close(db);
    return miss ? 1 : 0;
}
