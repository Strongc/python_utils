# coding: utf8
""" 命中概率为：1 - (1 - (1-K/N)**P)**T
1 - (1 - (1-10.0/256)**40)**60 = 0.99999879005606
1 - (1 - (1-20.0/256)**40)**60 = 0.90591116528251
"""
from permutations_128_64 import PERMUTATIONS_128_64
from permutations_256_128 import PERMUTATIONS_256_128

def permute_128_64(v):
    for p in PERMUTATIONS_128_64:
        yield ''.join([v[p[i]] for i in range(len(p))])

def permute_256_60(v):
    for p in PERMUTATIONS_256_128[:60]:
        yield ''.join([v[p[i]] for i in range(len(p))])

