#!/usr/bin/env python
# coding=utf-8

# class A(object):
#     a = 'a'
#     def __init__(self):
#         self.a = 'https://www.baidu.com'
#         pass 

#     @classmethod
#     def get(cls):
#         b = cls.test()
#         print b
#         return cls.a


#     @staticmethod
#     def test():
#         a = 'b'
#         return a

#     @property
#     def path_url(self):
#         url = self.a
#         if url:
#             url = url+'key'
#             return url

# a = A()
# print a.path_url
# print hasattr(a, 'path_url')

from functools import wraps

def memory_decorator(func):
    memory={}
    @wraps(func)
    def wrapper(*args, **kwargs):
        if args in memory:
            return memory.get(args)
        value = func(*args, **kwargs)
        memory[args] = value
        return value
    return wrapper

@memory_decorator


    

