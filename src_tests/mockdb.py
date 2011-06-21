#!/usr/bin/env python

import sys
import hashlib

md5 = lambda v: hashlib.md5(v).hexdigest()

tree = {}
for filename in sys.argv[1:]:
    for line in file(filename):
        t = line.split()
        if not t:
            continue

        action = t.pop(0)
        if action == 'set':
            key, value = t
            tree[key] = value
        elif action == 'del':
            key, = t
            if key in tree:
                del tree[key]
        elif action in ['write', 'reopen', 'gc']:
            pass
        else:
            assert False

for k, v in tree.iteritems():
    print "hash=%s key=%s value=%s" % (md5(k), k, v)
