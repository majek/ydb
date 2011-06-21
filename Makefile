CC:=gcc

CFLAGS=-std=c99 -pedantic -Wall -Wextra -g -fPIC -Isrc -O3 -march=native
LIBS=-lm -lcrypto

O_FILES=src/ydb_batch.o		\
	src/ydb_common.o	\
	src/ydb_logging.o	\
	src/ydb_writer.o	\
	src/ydb_file.o		\
	src/ydb_db.o		\
	src/ydb_record.o	\
	src/ydb_reader.o	\
	src/ydb_hashdir.o	\
	src/ydb_hashdir_active.o	\
	src/ydb_hashdir_frozen.o	\
	src/ydb_itree.o		\
	src/ohamt.o		\
	src/stddev.o		\
	src/memalloc.o		\
	src/ydb_log.o		\
	src/bitmap.o		\
	src/ydb_state.o		\
	src/ydb_logs.o		\
	src/hamt.o		\
	src/ydb_sys.o		\
	src/ydb_base.o		\
	src/ydb_base_aux.o	\
	src/ydb_base_pub.o	\
	src/ydb_public.o

TPROGS=src_tests/test_ydb_write	\
	src_tests/test_ydb_read


all: libydb.a $(TPROGS) tests

libydb.a: $(O_FILES)
	ar r $@ $^
	ranlib $@


src_tests/test_ydb_write: src_tests/test_ydb_write.o src_tests/test_common.o libydb.a
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS) $(LTEST)

src_tests/test_ydb_read: src_tests/test_ydb_read.o src_tests/test_common.o libydb.a
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS) $(LTEST)

# Cancel the implicit rule.
%.o: %.c

%.o: %.c
	$(CC) $(CFLAGS) -o $@ -c $<

clean::
	rm -f libydb.a $(TPROGS) src/*.o src_tests/*.o


tests/test-stress-gc.in:
	python ./src_tests/simple_generate.py 10000 1 1600 > $@
	rm -rf tests.mk

tests/test-overwrites.in:
	python ./src_tests/simple_generate.py 10000 1 1 > $@
	rm -rf tests.mk

tests:: tests/test-stress-gc.in tests/test-overwrites.in

tests.mk: src_tests/generate_makefile.py
	python src_tests/generate_makefile.py > tests.mk

include tests.mk
