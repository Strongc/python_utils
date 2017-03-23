# python-utils

## 目标
python 一些通用的类和方法

## 清单

+ common.py: 通用的方法

+ big_file_reader.py: 大文件读取
```
r = BigFileReader(path, trunk_size=1024 * 1024 * 32)
lines = r.readlines()
while lines:
....# do somethin
....lines = r.readlines()
```

+ exclusive_object.py: crontab 定期运行时保证只有一个实例在同时运行
```
if __name__ == '__main__':
....path = '%s/%s.pid' % (os.path.dirname(os.path.abspath(__file__)), sys.argv[0])
....eo = ExclusiveObject(path)
....# your codes here
```

+ proxy.py: 使用代理
```
headers = {'Proxy-Authorization': Proxy.get_header()}
r = requests.get(url=url, proxies=Proxy.get_proxies(), headers=headers)
```

+ myrequests.py: 使用proxy代理的requests

+ mysql.py: mysql api封装
```
db = MySql('rdsv48e18t98h6331x8d.mysql.rds.aliyuncs.com', 3306, 'bdtt', 'Chengzi123', 'bdtt')
print "select"
for r in db.select('local_region', ['id', 'name'], where="WHERE id>100"):
....id, name = r
print "execute"
sql = 'UPDATE tmp_local_region SET name=%s WHERE id=%s'
args = ('foo', '123456')
db.execute(sql, args)
print "executemany"
sql = 'INSERT INTO tmp_local_region (id, name) VALUES (%s, %s)'
arr = [(i, str(i)) for i in range(10000)]
for i in range(0, len(arr), 500):
....db.executemany(sql, arr[i:i+500])
```

+ timepicker.py: 时间获取
```
dt = timepicker.picktime(u'发布日期：2013-09-30')
print dt.strftime('%Y-%m-%d %H:%M:%S')
```
