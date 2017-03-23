# coding: utf8

import sys
import random

def gen(d, c):
    ks = {}
    while len(ks) < c:
        a = [i for i in range(d)]
        random.shuffle(a)
        k = ','.join([str(x) for x in a])
        if k not in ks:
            ks[k] = a

    content = '# coding: utf8 \n\n'
    content += '"""\n'
    content += 'this is an auto-generated file by gen_permutations.py\n'
    content += '"""\n\n\n'
    content += 'PERMUTATIONS_%d_%d = [\n' % (d, c)
    for k,v in ks.items():
        content += '    (%s), \n' % ', '.join([str(x) for x in v])
    content += ']'
    
    open('permutations_%d_%d.py' % (d, c), 'wb').write(content)

gen(int(sys.argv[1]), int(sys.argv[2]))

