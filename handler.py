
import pandas as pd
import pickle
import os

import boto3


AWS_CREDENTIAL = """
[default]
aws_access_key_id = AWS_ACCESS_KEY_ID
aws_secret_access_key = AWS_SECRET_ACCESS_KEY
"""


class S3():

    def __init__(self, bucket_name, key_path):

        self._bucket_name = bucket_name
        self._set_access_key(key_path)

        self.s3 = boto3.resource("s3")
        s3 = self.s3
        self.bucket = s3.Bucket(bucket_name)

    def _set_access_key(self, key_path):

        key_path = os.path.expanduser(key_path)
        aws_credential_path = os.path.expanduser("~/.aws/credentials")

        df = pd.read_csv(key_path, header=None)
        (id_, key) = (df.iloc[0, 0].split("=")[1],
                      df.iloc[1, 0].split("=")[1])

        content = AWS_CREDENTIAL
        content = content.replace("AWS_ACCESS_KEY_ID", id_)
        content = content.replace("AWS_SECRET_ACCESS_KEY", key)

        with open(aws_credential_path, "w") as f:
            f.write(content)

    def put_pickle(self, key, data):

        bucket = self.bucket

        temp_file_name = "-".join(key.split("/"))
        pd.to_pickle(data, temp_file_name)
        with open(temp_file_name, "rb") as f:
            data = f.read()

        bucket.put_object(Key=key, Body=data)
        os.remove(temp_file_name)

    def get_pickle(self, key):

        bucket_name = self._bucket_name
        s3 = self.s3
        
        obj = s3.Object(bucket_name=bucket_name, key=key)
        response = obj.get()
        data = response['Body'].read()
        return pickle.loads(data)

    def delete_key(self, key):
        
        bucket_name = self._bucket_name
        s3 = self.s3
        
        obj = s3.Object(bucket_name=bucket_name, key=key)
        obj.delete()

    def get_key_list(self):

        bucket = self.bucket

        keys = [obj.key for obj in bucket.objects.all()]
        return keys
