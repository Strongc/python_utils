# coding: utf8
import sys
from url_open3 import url_open
from webpage_open import webpage_open

url = sys.argv[1]
r = webpage_open(url, use_proxy=1)
print r.status_code

