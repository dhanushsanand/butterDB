/* pager.c — fixed 4 KiB page I/O on top of pread/pwrite. */
#include "internal.h"
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

int pager_open(pager_t *p, const char *path)
{
    p->fd = open(path, O_RDWR | O_CREAT, 0644);
    if (p->fd < 0) return -1;
    struct stat st;
    if (fstat(p->fd, &st) != 0) { close(p->fd); return -1; }
    p->npages = (pgno_t)(st.st_size / PAGE_SIZE);
    pthread_mutex_init(&p->lock, NULL);
    return 0;
}

void pager_close(pager_t *p)
{
    if (p->fd >= 0) close(p->fd);
    p->fd = -1;
    pthread_mutex_destroy(&p->lock);
}

int pager_read(pager_t *p, pgno_t pgno, void *buf)
{
    ssize_t n = pread(p->fd, buf, PAGE_SIZE, (off_t)pgno * PAGE_SIZE);
    if (n == PAGE_SIZE) return 0;
    if (n >= 0) { /* short read: page not fully written yet — treat as zeros */
        memset((uint8_t *)buf + n, 0, PAGE_SIZE - (size_t)n);
        return 0;
    }
    return -1;
}

int pager_write(pager_t *p, pgno_t pgno, const void *buf)
{
    ssize_t n = pwrite(p->fd, buf, PAGE_SIZE, (off_t)pgno * PAGE_SIZE);
    return n == PAGE_SIZE ? 0 : -1;
}

pgno_t pager_alloc(pager_t *p)
{
    static const uint8_t zero[PAGE_SIZE] = {0};
    pthread_mutex_lock(&p->lock);
    pgno_t pg = p->npages++;
    pthread_mutex_unlock(&p->lock);
    if (pager_write(p, pg, zero) != 0) return PGNO_NONE;
    return pg;
}

int pager_sync(pager_t *p)
{
    return fsync(p->fd) == 0 ? 0 : -1;
}
