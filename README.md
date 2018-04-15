# aws_handler

## python

```python

from aws_handler.handler import S3


s3 = S3("seohasong", "~/SEOHASONG/conf/key/rootkey.csv")
s3.put_pickle("test0", range(10))
s3.put_pickle("test1", range(10))
s3.put_pickle("test2", range(10))

print([key for key in s3.get_key_list()])
>>> ['test0', 'test1', 'test2']
    
s3.delete_key("test0")
s3.delete_key("test1")
s3.delete_key("test2")

print([key for key in s3.get_key_list()])
>>> []
```