#!/usr/bin/env python

import glob
import re

tests = []

for basename in glob.glob("tests/base-*.in"):
    base = re.match("[^-]*-(.*)[.]in", basename).group(1)
    for testname in glob.glob("tests/test-*.in"):
        test = re.match("[^-]*-(.*)[.]in", testname).group(1)
        print """
.PHONY: test-%(n)s
test-%(n)s: %(n)s-mock.out %(n)s-ydb.out
	@diff $^ > /dev/null && \\
		echo " [+] Test %(n)s: ok!" || \\
		(echo " [!] Test %(n)s: FAILED: diff $^"; exit 1;)

%(n)s-mock.out: %(basename)s %(testname)s
	python src_tests/mockdb.py $^ |sort > $@

.PHONY: %(n)s-ydb.out
%(n)s-ydb.out: %(basename)s %(testname)s
	@rm -rf /tmp/%(n)s-db
	cat $^ | ./src_tests/test_ydb_write /tmp/%(n)s-db
	./src_tests/test_ydb_read /tmp/%(n)s-db |sort > $@

clean_tests::
	rm -rf /tmp/%(n)s-db

""" % {'n': base + '-' + test,
       'base': base,
       'test': test,
       'testname': testname,
       'basename': basename}


        tests.append('test-' + base + '-' + test)

print
print "clean:: clean_tests"
print
print ".PHONY: tests"
print "tests:: %s" % (' '.join(tests),)
