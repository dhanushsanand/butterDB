/* cli.c — interactive shell for ButterDB.
 *   butterdb <file>            start a REPL
 *   butterdb <file> -c "cmd"   run one command and exit
 */
#include "butterdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int print_cb(const char *k, size_t kl, const void *v, size_t vl, void *ud)
{
    int *count = ud;
    printf("%.*s = %.*s\n", (int)kl, k, (int)vl, (const char *)v);
    (*count)++;
    return 0;
}

static int run(butterdb_t *db, char *line)
{
    char *cmd = strtok(line, " \t\r\n");
    if (!cmd) return 0;
    if (!strcmp(cmd, "put")) {
        char *k = strtok(NULL, " \t\r\n"), *v = strtok(NULL, "\r\n");
        if (!k || !v) { puts("usage: put <key> <value>"); return 0; }
        int rc = butterdb_put(db, k, strlen(k), v, strlen(v));
        puts(rc == BDB_OK ? "OK" : butterdb_strerror(rc));
    } else if (!strcmp(cmd, "get")) {
        char *k = strtok(NULL, " \t\r\n");
        if (!k) { puts("usage: get <key>"); return 0; }
        char buf[BDB_MAX_VAL + 1]; size_t vl = BDB_MAX_VAL;
        int rc = butterdb_get(db, k, strlen(k), buf, &vl);
        if (rc == BDB_OK) printf("%.*s\n", (int)vl, buf); else puts(butterdb_strerror(rc));
    } else if (!strcmp(cmd, "del")) {
        char *k = strtok(NULL, " \t\r\n");
        if (!k) { puts("usage: del <key>"); return 0; }
        int rc = butterdb_delete(db, k, strlen(k));
        puts(rc == BDB_OK ? "OK" : butterdb_strerror(rc));
    } else if (!strcmp(cmd, "scan")) {
        char *s = strtok(NULL, " \t\r\n");
        int n = 0;
        butterdb_scan(db, s, s ? strlen(s) : 0, print_cb, &n);
        printf("(%d rows)\n", n);
    } else if (!strcmp(cmd, "checkpoint")) {
        puts(butterdb_checkpoint(db) == BDB_OK ? "OK" : "error");
    } else if (!strcmp(cmd, "stats")) {
        bdb_stats_t s; butterdb_stats(db, &s);
        printf("puts=%llu gets=%llu hits=%llu dels=%llu splits=%llu\n"
               "pool hits=%llu misses=%llu evictions=%llu\n"
               "wal records=%llu bytes=%llu | pages=%u height=%u\n",
               (unsigned long long)s.puts, (unsigned long long)s.gets, (unsigned long long)s.hits,
               (unsigned long long)s.dels, (unsigned long long)s.splits,
               (unsigned long long)s.pool_hits, (unsigned long long)s.pool_misses,
               (unsigned long long)s.pool_evictions, (unsigned long long)s.wal_records,
               (unsigned long long)s.wal_bytes, s.pages, s.tree_height);
    } else if (!strcmp(cmd, "help")) {
        puts("put <k> <v> | get <k> | del <k> | scan [start] | checkpoint | stats | quit");
    } else if (!strcmp(cmd, "quit") || !strcmp(cmd, "exit")) {
        return 1;
    } else {
        puts("unknown command (try: help)");
    }
    return 0;
}

int main(int argc, char **argv)
{
    if (argc < 2) { fprintf(stderr, "usage: %s <dbfile> [-c \"command\"]\n", argv[0]); return 1; }
    butterdb_t *db;
    bdb_options_t o = { .pool_frames = 256, .sync_wal = 1 };
    int rc = butterdb_open(&db, argv[1], &o);
    if (rc != BDB_OK) { fprintf(stderr, "open failed: %s\n", butterdb_strerror(rc)); return 1; }
    if (argc >= 4 && !strcmp(argv[2], "-c")) {
        char line[512]; snprintf(line, sizeof line, "%s", argv[3]);
        run(db, line);
        butterdb_close(db);
        return 0;
    }
    printf("ButterDB — type 'help' for commands\n");
    char line[512];
    while (printf("butter> "), fflush(stdout), fgets(line, sizeof line, stdin)) {
        if (run(db, line)) break;
    }
    butterdb_close(db);
    return 0;
}
