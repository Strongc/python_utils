# coding: utf8
from errors import InvalidParamError


def get_param(params, key, key_type, raise_exception=True):
    value = params.get(key)
    if isinstance(value, key_type):
        return value
    if raise_exception:
        raise InvalidParamError('invalid param %s' % key)
    return None

