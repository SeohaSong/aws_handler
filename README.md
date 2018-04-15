# aws_handler

## python

```python

from aws_handler.handler import S3


s3 = S3("BUCKER_NAME", "ACCESS_KEY_PATH")
s3.put_pickle("test0", range(10))
s3.put_pickle("test1", range(10))
s3.put_pickle("test2", range(10))

for key in s3.get_key_list():
    print(key)
    
s3.delete_key("test0")
s3.delete_key("test1")
s3.delete_key("test2")

```

```
['test0', 'test1', 'test2']
[]
```