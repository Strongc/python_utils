#!/usr/bin/env python
# coding=utf-8

"""
Wechat.exceptions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module contains the exceptions which occur in 
weixin spider process.
"""


class NoResultFind(ValueError):
    """If sogou engine has no result occur this exception."""
    pass

class UrlError(ValueError):
    """if url is `NoneType` or '' occur this exception."""
    pass

class GetContentError(ValueError):
    """Get content from url error."""
    pass

class GetElementError(ValueError):
    """Get element info from content error."""
    pass

class ParseElementError(BaseException):
    """Get element using xpath failed"""
    pass

