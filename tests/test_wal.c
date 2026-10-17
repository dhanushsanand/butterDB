/* Crash recovery: writes after the last checkpoint must survive a crash via WAL
 * replay, and a torn record at the WAL tail must be ignored. */
#include "butterdb.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define DB "test_wal.db"
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #x); exit(1); } } while (0)

int main(void)
{
    remove(DB); remove(DB ".wal");
    butterdb_t *db; char k[32], v[64], out[128]; size_t vl;
    bdb_options_t o = { .pool_frames = 32, .sync_wal = 1 };

    CHECK(butterdb_open(&db, DB, &o) == BDB_OK);
    for (int i = 0; i < 1000; i++) {
        snprintf(k, sizeof k, "a%05d", i); snprintf(v, sizeof v, "v%d", i);
        CHECK(butterdb_put(db, k, strlen(k), v, strlen(v)) == BDB_OK);
    }
    CHECK(butterdb_checkpoint(db) == BDB_OK);
    /* post-checkpoint writes: puts, an overwrite, deletes */
    for (int i = 1000; i < 3000; i++) {
        snprintf(k, sizeof k, "a%05d", i); snprintf(v, sizeof v, "v%d", i);
        CHECK(butterdb_put(db, k, strlen(k), v, strlen(v)) == BDB_OK);
    }
    CHECK(butterdb_put(db, "a00001", 6, "changed", 7) == BDB_OK);
    for (int i = 0; i < 500; i++) { snprintf(k, sizeof k, "a%05d", i * 2); butterdb_delete(db, k, strlen(k)); }
    butterdb_crash(db);                      /* nothing flushed, no checkpoint */

    /* simulate a torn write by appending a partial record to the WAL */
    int fd = open(DB ".wal", O_WRONLY | O_APPEND); CHECK(fd >= 0);
    CHECK(write(fd, "\x10\x00\x00\x00\x40\x00\x01\x05", 8) == 8); close(fd);

    CHECK(butterdb_open(&db, DB, &o) == BDB_OK);
    for (int i = 0; i < 3000; i++) {
        snprintf(k, sizeof k, "a%05d", i); snprintf(v, sizeof v, "v%d", i); vl = sizeof out;
        int rc = butterdb_get(db, k, strlen(k), out, &vl);
        if (i < 1000 && i % 2 == 0) { CHECK(rc == BDB_NOTFOUND); continue; }
        CHECK(rc == BDB_OK);
        if (i == 1) CHECK(vl == 7 && !memcmp(out, "changed", 7));
        else        CHECK(vl == strlen(v) && !memcmp(out, v, vl));
    }
    struct stat st; CHECK(stat(DB ".wal", &st) == 0 && st.st_size == 0);   /* WAL truncated after recovery */
    butterdb_close(db);
    remove(DB); remove(DB ".wal");
    puts("test_wal: PASS");
    return 0;
}
