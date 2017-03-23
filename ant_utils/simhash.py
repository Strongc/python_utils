# -*- coding: utf-8 -*-

def string_hash(v, hashbits=128):
    if v == "":
        return 0
    else:
        x = ord(v[0])<<7
        m = 1000003
        mask = 2**hashbits-1
        for c in v:
            x = ((x*m)^ord(c)) & mask
        x ^= len(v)
        if x == -1:
            x = -2
        return x

def simhash(tokens, hashbits=128):
    v = [0] * hashbits

    for t in [string_hash(x, hashbits) for x in tokens]:
        bitmask = 0
        for i in range(hashbits):
            bitmask = 1 << i
            if t & bitmask:
                v[i] += 1
            else:
                v[i] += -1

    return ''.join(['1' if v[i] >= 0 else '0' for i in range(hashbits)])
