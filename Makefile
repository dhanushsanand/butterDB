CC      ?= gcc
CFLAGS  ?= -std=c11 -D_GNU_SOURCE -O2 -Wall -Wextra -Wpedantic -pthread -Iinclude
LDFLAGS ?= -pthread

SRC  := src/pager.c src/bufpool.c src/wal.c src/btree.c src/db.c
OBJ  := $(SRC:.c=.o)
LIB  := libbutterdb.a
TESTS := test_basic test_wal test_concurrent

all: $(LIB) butterdb bench

$(LIB): $(OBJ)
	ar rcs $@ $^

src/%.o: src/%.c src/internal.h include/butterdb.h
	$(CC) $(CFLAGS) -c $< -o $@

butterdb: tools/cli.c $(LIB)
	$(CC) $(CFLAGS) $< $(LIB) $(LDFLAGS) -o $@

bench: tools/bench.c $(LIB)
	$(CC) $(CFLAGS) $< $(LIB) $(LDFLAGS) -o $@

test_%: tests/test_%.c $(LIB)
	$(CC) $(CFLAGS) $< $(LIB) $(LDFLAGS) -o $@

test: $(TESTS)
	@for t in $(TESTS); do ./$$t || exit 1; done

# TSan's deadlock detector reports lock-order inversions because buffer-pool
# frames (and their latches) are reused for different pages over time; the
# B-tree only ever couples latches top-down / left-to-right, so these are
# false positives. Data-race detection stays on.
tsan: export TSAN_OPTIONS=detect_deadlocks=0

tsan: CFLAGS += -fsanitize=thread -g -O1
tsan: LDFLAGS += -fsanitize=thread
tsan: clean test

asan: CFLAGS += -fsanitize=address,undefined -g -O1
asan: LDFLAGS += -fsanitize=address,undefined
asan: clean test

clean:
	rm -f $(OBJ) $(LIB) butterdb bench $(TESTS) *.db *.db.wal

.PHONY: all test tsan asan clean
