
import time
import sys
import pandas as pd
import asyncio
import os

import boto3


class S3():

    def __init__(self, bucket_name, key_path):
        self._initialize(bucket_name, key_path)

    def _initialize(self, bucket_name, key_path):
        self._bucket_name = bucket_name
        self._set_access_key(key_path)
        self._s3 = boto3.resource("s3")
        self._bucket = self._s3.Bucket(bucket_name)

    def _set_access_key(self, key_path):

        key_path = os.path.expanduser(key_path)

        aws_credential_path = os.path.expanduser("~/.aws/credentials")
        aws_dir = os.path.split(aws_credential_path)[0]
        if not os.path.isdir(aws_dir):
            os.makedirs(aws_dir)

        df = pd.read_csv(key_path)
        if len(df.columns) == 1:
            df = pd.read_csv(key_path, sep="=", index_col=0, header=None)
            df = df.transpose()
            df.columns = ["Access key ID", "Secret access key"]
        id_ = df.iloc[0]["Access key ID"]
        key = df.iloc[0]["Secret access key"]

        content = AWS_CREDENTIAL_TEXT
        content = content.replace("AWS_ACCESS_KEY_ID", id_)
        content = content.replace("AWS_SECRET_ACCESS_KEY", key)

        with open(aws_credential_path, "w") as f:
            f.write(content)

    def get_key(self, key, signal=False, data=None):
        bucket_name = self._bucket_name
        s3 = self._s3

        def show_status(i=0):
            nonlocal signal
            nonlocal data
            while True:
                i += 1
                time.sleep(0.5)
                sys.stdout.write("\r[aws_handler] Loading .% -3s"%("."*(i%3)))
                if signal:
                    sys.stdout.write("\r[aws_handler] Loading complete\n")
                    break

        def get_data():
            nonlocal signal
            nonlocal data
            obj = s3.Object(bucket_name=bucket_name, key=key)
            response = obj.get()
            data = response['Body'].read()
            signal = True
            
        async def main():
            futures = [
                loop.run_in_executor(None, show_status),
                loop.run_in_executor(None, get_data)
            ]
            await asyncio.wait(futures)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())

        return data

    def put_key(self, key, filepath):
        bucket = self._bucket
        bucket.put_object(Key=key,
                          Body=open(filepath, "rb").read())

    def delete_key(self, key):
        bucket_name = self._bucket_name
        s3 = self._s3
        obj = s3.Object(bucket_name=bucket_name, key=key)
        obj.delete()

    def get_key_list(self):
        bucket = self._bucket
        keys = [obj.key for obj in bucket.objects.all()]
        return keys


AWS_CREDENTIAL_TEXT = """
[default]
aws_access_key_id = AWS_ACCESS_KEY_ID
aws_secret_access_key = AWS_SECRET_ACCESS_KEY
"""