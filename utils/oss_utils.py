## upload pic to ossutils
## for qiniu or oss

import oss2
import uuid
from itertools import islice
#upload image to oss service
#input param: absolute path
#return: oss outer link

def getBucket(bucket_name = None):
    if bucket_name is None:
        bucket_name = 'bdtt-api'
    oss_endpoint = 'oss-cn-hangzhou.aliyuncs.com'
    oss_key_id = 'RBbUeiN8FHcPc0As'
    oss_key_value = '6Not2pLjT1drEr4imxiCXgieZTpXgs'
    auth = oss2.Auth(oss_key_id, oss_key_value)
    bucket = oss2.Bucket(auth, 'http://'+oss_endpoint, bucket_name)
    return bucket

def pushPicToOss(path, bucket, bucket_name = None):
    if bucket_name is None:
        bucket_name = 'bdtt-api'
    oss_endpoint = 'oss-cn-hangzhou.aliyuncs.com'
    oss_key = str(uuid.uuid1())
    bucket.put_object_from_file(oss_key, path)
    url_head = 'http://img.benditoutiao.com/'
    return url_head+oss_key, oss_key

def getObjectRandom(bucket, num=10):
    item = []
    for b in islice(oss2.ObjectIterator(bucket), num):
        item.append(b.key)
    return item

def findObject(bucket, prefix):
    item = []
    for obj in oss2.ObjectIterator(bucket, prefix=prefix):
        print obj.key

def removeObject(bucket, oss_key):
    bucket.delete_object(oss_key)

if __name__ == '__main__':
    bucket = getBucket()
    # print getObjectRandom(bucket,20)
    [linkUrl,key]  = pushPicToOss("/Users/guxiangyu/Downloads/Aerial03.jpg", bucket)
    print linkUrl
    # print linkUrl
    findObject(bucket, key)
    #removeObject(bucket, key)
    findObject(bucket, key)
