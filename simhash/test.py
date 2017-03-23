# coding: utf8

from simhash import Simhash

s1 = Simhash('abcdefghijklmnopqrstuvwxyz', 128)
s2 = Simhash('abcdefghijklmnopqrstuvwayz', 128)
print s1.value
print s2.value
print s1.distance(s2)
