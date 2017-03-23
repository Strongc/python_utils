## upload pic to ossutils
## for qiniu or oss

import redis
#upload image to oss service

def exist(Redis, item):
    if Redis.exists(item):
        return True
    else:
        return False

def set(Redis, item, value):
    Redis.set(item, value)

def get(Redis, item):
    if exist(Redis, item):
        return Redis.get(item)

def delete(Redis, item):
    if exist(Redis, item):
        Redis.delete(item)

if __name__ == '__main__':
    Redis = redis.StrictRedis(host='117.149.19.19',
            password="0f6beec32dac43de:Chengzi123", port=6379, db=4)

    set(Redis, "test1", "haha")

    print get(Redis, "test1")

    print get(Redis, "test2")