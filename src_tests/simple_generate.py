#!/usr/bin/env python

import random
import string
import sys

assert len(sys.argv) == 4

items = int(sys.argv[1])
key_mu = int(sys.argv[2])
value_mu = int(sys.argv[3])


rand_str = lambda l: ''.join(random.choice(string.lowercase) for i in xrange(l))


uniq = random.randint(0, 1 << 32)
val = '_' * value_mu

actions = [
    'set', 'set', 'set', 'set', 'set', 'set', 'set', 'set', 'set', 'set',
    'set', 'set', 'set', 'set', 'set', 'set', 'set', 'set', 'set', 'set',
    'del', 'del', 'del', 'del',
    'write',
    ]

for i in xrange(items):
    action = random.choice(actions)
    if action is 'set':
        uniq += 1
        print "set", rand_str(key_mu), str(uniq) + val
    elif action is 'del':
        print "del", rand_str(key_mu)
    elif action is 'write':
        print "write"
    else:
        assert 0
