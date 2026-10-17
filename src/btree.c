/* btree.c — B+tree over buffer-pool pages, with latch crabbing.
 *
 * Page layouts (4096 bytes):
 *   leaf:     hdr(12) | keys[25][32] | vals[25][128]          = 4012
 *   internal: hdr(12) | keys[113][32] | children[114][4]      = 4084
 *
 * Concurrency ("latch crabbing" / lock coupling):
 *   readers  take read latches top-down, releasing the parent as soon as the
 *            child latch is held.
 *   put      takes write latches top-down and keeps a stack of ancestors;
 *            whenever it reaches a node that cannot split (nkeys < MAX) all
 *            ancestors are released, because the split can never propagate
 *            past a non-full node.
 *   delete   is lazy (no merge/rebalance), so it only ever writes the leaf:
 *            read latches down, write latch on the leaf.
 *   root     changes are guarded by t->root_latch, which also plays the role
 *            of the root's "parent" for crabbing purposes.
 */
#include "internal.h"
#include <string.h>

#define HDR          sizeof(node_hdr_t)
#define LEAF_MAX     25
#define INT_MAX_KEYS 113
#define MAX_DEPTH    64

_Static_assert(HDR == 12, "node header");
_Static_assert(HDR + LEAF_MAX * (KEY_SIZE + VAL_SLOT) <= PAGE_SIZE, "leaf fits");
_Static_assert(HDR + INT_MAX_KEYS * KEY_SIZE + (INT_MAX_KEYS + 1) * sizeof(pgno_t) <= PAGE_SIZE, "internal fits");
_Static_assert((HDR + INT_MAX_KEYS * KEY_SIZE) % 4 == 0, "children aligned");

static inline node_hdr_t *HDRP(frame_t *f)      { return (node_hdr_t *)f->data; }
static inline uint8_t    *LKEY(frame_t *f, int i){ return f->data + HDR + (size_t)i * KEY_SIZE; }
static inline uint8_t    *LVAL(frame_t *f, int i){ return f->data + HDR + LEAF_MAX * KEY_SIZE + (size_t)i * VAL_SLOT; }
static inline uint8_t    *IKEY(frame_t *f, int i){ return f->data + HDR + (size_t)i * KEY_SIZE; }
static inline pgno_t     *ICHILD(frame_t *f)     { return (pgno_t *)(f->data + HDR + INT_MAX_KEYS * KEY_SIZE); }
static inline int         is_leaf(frame_t *f)    { return HDRP(f)->type == NODE_LEAF; }

static void key_pad(uint8_t out[KEY_SIZE], const char *key, size_t klen)
{
    memset(out, 0, KEY_SIZE);
    memcpy(out, key, klen);
}
static inline int key_cmp(const uint8_t *a, const uint8_t *b) { return memcmp(a, b, KEY_SIZE); }
static inline size_t key_len(const uint8_t *k) { return strnlen((const char *)k, KEY_SIZE); }

/* first index with keys[i] >= key */
static int leaf_lower_bound(frame_t *f, const uint8_t *key)
{
    int lo = 0, hi = HDRP(f)->nkeys;
    while (lo < hi) {
        int mid = (lo + hi) / 2;
        if (key_cmp(LKEY(f, mid), key) < 0) lo = mid + 1; else hi = mid;
    }
    return lo;
}
/* child index for key: number of separator keys <= key */
static int int_child_index(frame_t *f, const uint8_t *key)
{
    int lo = 0, hi = HDRP(f)->nkeys;
    while (lo < hi) {
        int mid = (lo + hi) / 2;
        if (key_cmp(IKEY(f, mid), key) <= 0) lo = mid + 1; else hi = mid;
    }
    return lo;
}

static void release(bufpool_t *bp, frame_t *f, int dirty)
{
    bp_unlatch(f);
    bp_unpin(bp, f, dirty);
}

/* --------------------------------------------------------------- lifecycle */
int btree_init_empty(btree_t *t, bufpool_t *bp, pgno_t *root_out)
{
    t->bp = bp;
    t->splits = 0;
    pthread_rwlock_init(&t->root_latch, NULL);
    pgno_t pg;
    frame_t *f = bp_new(bp, &pg);
    if (!f) return -1;
    HDRP(f)->type = NODE_LEAF;
    HDRP(f)->nkeys = 0;
    HDRP(f)->next = PGNO_NONE;
    bp_unpin(bp, f, 1);
    t->root = pg;
    *root_out = pg;
    return 0;
}

int btree_attach(btree_t *t, bufpool_t *bp, pgno_t root)
{
    t->bp = bp;
    t->root = root;
    t->splits = 0;
    pthread_rwlock_init(&t->root_latch, NULL);
    return 0;
}

void btree_destroy(btree_t *t) { pthread_rwlock_destroy(&t->root_latch); }

/* ------------------------------------------------------------------- read */
/* Descend with read latches to the leaf that should contain `key`.
 * If leaf_write is set the leaf itself is write-latched. Returns pinned+latched leaf. */
static frame_t *descend_read(btree_t *t, const uint8_t *key, int leaf_write)
{
    bufpool_t *bp = t->bp;
    pthread_rwlock_rdlock(&t->root_latch);
    frame_t *f = bp_fetch(bp, t->root);
    if (!f) { pthread_rwlock_unlock(&t->root_latch); return NULL; }
    if (is_leaf(f) && leaf_write) bp_wrlatch(f); else bp_rdlatch(f);
    pthread_rwlock_unlock(&t->root_latch);

    while (!is_leaf(f)) {
        pgno_t child = ICHILD(f)[int_child_index(f, key)];
        frame_t *c = bp_fetch(bp, child);
        if (!c) { release(bp, f, 0); return NULL; }
        if (is_leaf(c) && leaf_write) bp_wrlatch(c); else bp_rdlatch(c);
        release(bp, f, 0);
        f = c;
    }
    return f;
}

int btree_get(btree_t *t, const char *key, size_t klen, void *val, size_t *vlen)
{
    uint8_t k[KEY_SIZE];
    key_pad(k, key, klen);
    frame_t *leaf = descend_read(t, k, 0);
    if (!leaf) return BDB_EBUSY;
    int i = leaf_lower_bound(leaf, k);
    int rc = BDB_NOTFOUND;
    if (i < HDRP(leaf)->nkeys && key_cmp(LKEY(leaf, i), k) == 0) {
        uint8_t *slot = LVAL(leaf, i);
        uint16_t len;
        memcpy(&len, slot, 2);
        if (val && vlen) {
            size_t n = len < *vlen ? len : *vlen;
            memcpy(val, slot + 2, n);
        }
        if (vlen) *vlen = len;
        rc = BDB_OK;
    }
    release(t->bp, leaf, 0);
    return rc;
}

int btree_scan(btree_t *t, const char *start, size_t slen, bdb_scan_cb cb, void *ud)
{
    uint8_t k[KEY_SIZE];
    key_pad(k, start ? start : "", start ? slen : 0);
    frame_t *leaf = descend_read(t, k, 0);
    if (!leaf) return BDB_EBUSY;
    int i = leaf_lower_bound(leaf, k);
    for (;;) {
        for (; i < HDRP(leaf)->nkeys; i++) {
            uint8_t *kp = LKEY(leaf, i), *vp = LVAL(leaf, i);
            uint16_t len;
            memcpy(&len, vp, 2);
            if (cb((const char *)kp, key_len(kp), vp + 2, len, ud) != 0) {
                release(t->bp, leaf, 0);
                return BDB_OK;
            }
        }
        pgno_t next = HDRP(leaf)->next;
        if (next == PGNO_NONE) break;
        frame_t *n = bp_fetch(t->bp, next);
        if (!n) { release(t->bp, leaf, 0); return BDB_EBUSY; }
        bp_rdlatch(n);
        release(t->bp, leaf, 0);
        leaf = n;
        i = 0;
    }
    release(t->bp, leaf, 0);
    return BDB_OK;
}

uint32_t btree_height(btree_t *t)
{
    uint32_t h = 1;
    pthread_rwlock_rdlock(&t->root_latch);
    frame_t *f = bp_fetch(t->bp, t->root);
    if (!f) { pthread_rwlock_unlock(&t->root_latch); return 0; }
    bp_rdlatch(f);
    pthread_rwlock_unlock(&t->root_latch);
    while (!is_leaf(f)) {
        frame_t *c = bp_fetch(t->bp, ICHILD(f)[0]);
        if (!c) break;
        bp_rdlatch(c);
        release(t->bp, f, 0);
        f = c;
        h++;
    }
    release(t->bp, f, 0);
    return h;
}

/* ------------------------------------------------------------------ write */
static void leaf_insert_at(frame_t *f, int i, const uint8_t *key, const void *val, size_t vlen)
{
    int n = HDRP(f)->nkeys;
    memmove(LKEY(f, i + 1), LKEY(f, i), (size_t)(n - i) * KEY_SIZE);
    memmove(LVAL(f, i + 1), LVAL(f, i), (size_t)(n - i) * VAL_SLOT);
    memcpy(LKEY(f, i), key, KEY_SIZE);
    uint16_t len = (uint16_t)vlen;
    memcpy(LVAL(f, i), &len, 2);
    memcpy(LVAL(f, i) + 2, val, vlen);
    HDRP(f)->nkeys = (uint16_t)(n + 1);
}

static void int_insert_at(frame_t *f, int i, const uint8_t *sep, pgno_t child)
{
    int n = HDRP(f)->nkeys;
    pgno_t *ch = ICHILD(f);
    memmove(IKEY(f, i + 1), IKEY(f, i), (size_t)(n - i) * KEY_SIZE);
    memmove(&ch[i + 2], &ch[i + 1], (size_t)(n - i) * sizeof(pgno_t));
    memcpy(IKEY(f, i), sep, KEY_SIZE);
    ch[i + 1] = child;
    HDRP(f)->nkeys = (uint16_t)(n + 1);
}

typedef struct { frame_t *fr[MAX_DEPTH]; int n; } stack_t;

static void stack_release_ancestors(btree_t *t, stack_t *s, int *root_held)
{
    /* release everything except the top of stack */
    for (int i = 0; i < s->n - 1; i++) release(t->bp, s->fr[i], 0);
    s->fr[0] = s->fr[s->n - 1];
    s->n = 1;
    if (*root_held) { pthread_rwlock_unlock(&t->root_latch); *root_held = 0; }
}

static void stack_release_all(btree_t *t, stack_t *s, int *root_held)
{
    for (int i = 0; i < s->n; i++) release(t->bp, s->fr[i], 0);
    s->n = 0;
    if (*root_held) { pthread_rwlock_unlock(&t->root_latch); *root_held = 0; }
}

int btree_put(btree_t *t, const char *key, size_t klen, const void *val, size_t vlen)
{
    bufpool_t *bp = t->bp;
    uint8_t k[KEY_SIZE];
    key_pad(k, key, klen);

    stack_t st = { .n = 0 };
    int root_held = 1;
    pthread_rwlock_wrlock(&t->root_latch);
    frame_t *f = bp_fetch(bp, t->root);
    if (!f) { pthread_rwlock_unlock(&t->root_latch); return BDB_EBUSY; }
    bp_wrlatch(f);
    st.fr[st.n++] = f;

    /* descend with write latches, releasing ancestors at safe nodes */
    while (!is_leaf(f)) {
        if (HDRP(f)->nkeys < INT_MAX_KEYS) stack_release_ancestors(t, &st, &root_held);
        pgno_t child = ICHILD(f)[int_child_index(f, k)];
        frame_t *c = bp_fetch(bp, child);
        if (!c || st.n >= MAX_DEPTH) { if (c) bp_unpin(bp, c, 0); stack_release_all(t, &st, &root_held); return BDB_EBUSY; }
        bp_wrlatch(c);
        st.fr[st.n++] = c;
        f = c;
    }
    if (HDRP(f)->nkeys < LEAF_MAX) stack_release_ancestors(t, &st, &root_held);

    /* update in place? */
    int i = leaf_lower_bound(f, k);
    if (i < HDRP(f)->nkeys && key_cmp(LKEY(f, i), k) == 0) {
        uint16_t len = (uint16_t)vlen;
        memcpy(LVAL(f, i), &len, 2);
        memcpy(LVAL(f, i) + 2, val, vlen);
        for (int j = 0; j < st.n - 1; j++) release(bp, st.fr[j], 0);
        release(bp, f, 1);
        if (root_held) pthread_rwlock_unlock(&t->root_latch);
        return BDB_OK;
    }
    /* simple insert */
    if (HDRP(f)->nkeys < LEAF_MAX) {
        leaf_insert_at(f, i, k, val, vlen);
        release(bp, f, 1);
        st.n--;
        assert(st.n == 0 && !root_held);
        return BDB_OK;
    }

    /* ---- leaf split ----
     * Every node still on the stack is full and will split, plus possibly a
     * new root. Reserve all the pages up front so a pool-exhaustion failure
     * can never leave the tree half-restructured. */
    /* If the root latch was released, the bottom of the stack is a safe
     * (non-full) node that absorbs the split; otherwise a new root is needed. */
    int need = root_held ? st.n + 1 : st.n - 1, got = 0;
    frame_t *newf[MAX_DEPTH + 1]; pgno_t newpg[MAX_DEPTH + 1];
    for (; got < need; got++) {
        newf[got] = bp_new(bp, &newpg[got]);
        if (!newf[got]) {
            for (int j = 0; j < got; j++) bp_unpin(bp, newf[j], 0);
            stack_release_all(t, &st, &root_held);
            return BDB_EBUSY;
        }
    }
    int used = 0;
    frame_t *r = newf[used]; pgno_t rpg = newpg[used]; used++;
    int mid = LEAF_MAX / 2;
    int nr = LEAF_MAX - mid;
    HDRP(r)->type = NODE_LEAF;
    HDRP(r)->nkeys = (uint16_t)nr;
    HDRP(r)->next = HDRP(f)->next;
    memcpy(LKEY(r, 0), LKEY(f, mid), (size_t)nr * KEY_SIZE);
    memcpy(LVAL(r, 0), LVAL(f, mid), (size_t)nr * VAL_SLOT);
    HDRP(f)->nkeys = (uint16_t)mid;
    HDRP(f)->next = rpg;
    if (key_cmp(k, LKEY(r, 0)) < 0) leaf_insert_at(f, leaf_lower_bound(f, k), k, val, vlen);
    else                            leaf_insert_at(r, leaf_lower_bound(r, k), k, val, vlen);
    uint8_t sep[KEY_SIZE];
    memcpy(sep, LKEY(r, 0), KEY_SIZE);
    pgno_t newchild = rpg;
    bp_unpin(bp, r, 1);          /* r was never latched: nobody can reach it until linked */
    __atomic_add_fetch(&t->splits, 1, __ATOMIC_RELAXED);
    release(bp, f, 1);
    st.n--;

    /* ---- propagate upward ---- */
    while (st.n > 0) {
        frame_t *p = st.fr[st.n - 1];
        if (HDRP(p)->nkeys < INT_MAX_KEYS) {
            int_insert_at(p, int_child_index(p, sep), sep, newchild);
            release(bp, p, 1);
            st.n--;
            assert(st.n == 0 && !root_held && used == need);
            return BDB_OK;
        }
        /* split internal node p */
        frame_t *nn = newf[used]; pgno_t npg = newpg[used]; used++;
        int m = INT_MAX_KEYS / 2;               /* 56 */
        int rn = INT_MAX_KEYS - m - 1;          /* 56 */
        HDRP(nn)->type = NODE_INTERNAL;
        HDRP(nn)->nkeys = (uint16_t)rn;
        memcpy(IKEY(nn, 0), IKEY(p, m + 1), (size_t)rn * KEY_SIZE);
        memcpy(ICHILD(nn), &ICHILD(p)[m + 1], (size_t)(rn + 1) * sizeof(pgno_t));
        uint8_t newsep[KEY_SIZE];
        memcpy(newsep, IKEY(p, m), KEY_SIZE);
        HDRP(p)->nkeys = (uint16_t)m;
        if (key_cmp(sep, newsep) < 0) int_insert_at(p,  int_child_index(p, sep),  sep, newchild);
        else                          int_insert_at(nn, int_child_index(nn, sep), sep, newchild);
        bp_unpin(bp, nn, 1);
        release(bp, p, 1);
        st.n--;
        memcpy(sep, newsep, KEY_SIZE);
        newchild = npg;
        __atomic_add_fetch(&t->splits, 1, __ATOMIC_RELAXED);
    }

    /* ---- root split: we still hold root_latch exclusively ---- */
    assert(root_held && used == need - 1);
    frame_t *nr_ = newf[used]; pgno_t rootpg = newpg[used];
    HDRP(nr_)->type = NODE_INTERNAL;
    HDRP(nr_)->nkeys = 1;
    memcpy(IKEY(nr_, 0), sep, KEY_SIZE);
    ICHILD(nr_)[0] = t->root;
    ICHILD(nr_)[1] = newchild;
    bp_unpin(bp, nr_, 1);
    t->root = rootpg;
    pthread_rwlock_unlock(&t->root_latch);
    return BDB_OK;
}

int btree_delete(btree_t *t, const char *key, size_t klen)
{
    uint8_t k[KEY_SIZE];
    key_pad(k, key, klen);
    frame_t *leaf = descend_read(t, k, 1);
    if (!leaf) return BDB_EBUSY;
    int n = HDRP(leaf)->nkeys;
    int i = leaf_lower_bound(leaf, k);
    if (i >= n || key_cmp(LKEY(leaf, i), k) != 0) { release(t->bp, leaf, 0); return BDB_NOTFOUND; }
    memmove(LKEY(leaf, i), LKEY(leaf, i + 1), (size_t)(n - i - 1) * KEY_SIZE);
    memmove(LVAL(leaf, i), LVAL(leaf, i + 1), (size_t)(n - i - 1) * VAL_SLOT);
    HDRP(leaf)->nkeys = (uint16_t)(n - 1);
    release(t->bp, leaf, 1);
    return BDB_OK;
}
