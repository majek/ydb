#!/usr/bin/env python

import random
import string
import sys

random.seed(42)

assert len(sys.argv) == 4

items = int(sys.argv[1])
key_mu = int(sys.argv[2])
value_mu = int(sys.argv[3])


my_gauss = lambda mu: max(1,int(random.gauss(mu, mu * 0.35)))
rand_str = lambda l: ''.join(random.choice(string.lowercase) \
                                 for i in xrange(my_gauss(l)))

actions = [
    'set', 'set', 'set', 'set', 'set', 'set', 'set', 'set', 'set', 'set',
    'set', 'set', 'set', 'set', 'set', 'set', 'set', 'set', 'set', 'set',
    'del', 'del', 'del', 'del',
    'write',
    ]

for i in xrange(items):
    action = random.choice(actions)
    if action is 'set':
        print "set", rand_str(key_mu), \
            '%x' % (random.randint(0, 1<<16),) + '_' * my_gauss(value_mu)
    elif action is 'del':
        print "del", rand_str(key_mu)
    elif action is 'write':
        print "write"
    else:
        assert 0
