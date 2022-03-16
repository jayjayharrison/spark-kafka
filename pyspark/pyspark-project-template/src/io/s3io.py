import os
import logging
import boto3
import io
import json
import re
from botocore.exceptions import ClientError
from datetime import datetime
import typing


class S3IO:
    '''
    export AWS_ACCESS_KEY_ID='...'
    export AWS_SECRET_ACCESS_KEY='...'
    or
    set in .aws/credential
    or
    set in client paramenter
            's3',
            aws_access_key_id='xxx',
            aws_secret_access_key='xxx',
            endpoint_url='http://192.168.2.32:30021/'
    '''

    def __init__(self, endpoint_url=None):
        self.s3client: boto3.client = self.get_client(endpoint_url)

    @staticmethod
    def get_client(endpoint_url) -> boto3.client:
        # endpoint_url='http://192.168.3.34:30021/'
        client = boto3.client(
            's3',
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            endpoint_url=endpoint_url
        )
        return client

    def create_bucket(self, bucket, region=None) -> bool:
        """Create an S3 bucket in a specified region
        :param bucket: Bucket to create
        :param region: String region to create bucket in, e.g., 'us-west-2', default: (us-east-1)
        :return: True if bucket created, else False
        """
        try:
            if region is None:

                self.s3client.create_bucket(Bucket=bucket)
            else:

                location = {'LocationConstraint': region}
                self.s3client.create_bucket(Bucket=bucket,
                                            CreateBucketConfiguration=location)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def list_bucket(self, reverse=False) -> typing.List:
        """
        :param asc: order by
        :return: [{ 'Name': 'string1','CreationDate': '20150101'}]
        """
        response = self.s3client.list_buckets()

        if response['Buckets']:
            res = sorted(response['Buckets'], key=lambda x: x['CreationDate'], reverse=reverse)
            # stringify datetime
            for b in res:
                b['CreationDate'] = b['CreationDate'].strftime("%Y%m%d")
        else:
            res = []

        res = [b['Name'] for b in res]
        return res

    def upload_file(self, file_name, bucket, key=None) -> bool:
        """
        upload file from local
        :param file_name: local filename
        """
        # If S3 key was not specified, use file_name
        if key is None:
            key = os.path.basename(file_name)

        if not os.path.exists(file_name):
            raise Exception("file does not exist: " + file_name)

        try:
            self.s3client.upload_file(Filename=file_name, Bucket=bucket, Key=key)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def put_object(self, content: str, bucket: str, key: str) -> bool:
        """
        write content to object,
        :param key: data/file.txt
        :param content: can be dict, str, list, byte
        """

        CONTENT_TYPE = "text/plain;charset=utf-8"
        if isinstance(content, bytes):
            content = content.decode('utf-8')

        if not isinstance(content, str):
            content = json.dumps(content, ensure_ascii=False)

        try:
            response = self.s3client.put_object(Body=content, Bucket=bucket, Key=key, ContentType=CONTENT_TYPE)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def read_object(self, bucket: str, key: str, as_stream: bool = False):
        """
        :param as_stream: if true, return as StreamBody object
        :return content:str
        """
        try:
            response = self.s3client.get_object(Bucket=bucket, Key=key)
        except ClientError as e:
            logging.error(e)
            return False

        streaming_body = response['Body']

        if as_stream:
            return streaming_body

        else:
            body_string = streaming_body.read().decode('utf-8')  # .decode('unicode_escape')
            return body_string

    def list_objects(self, bucket, prefix: str = '', order_desc=False, recursive=False):
        """
        :param prefix:  Limits the response to keys that begin with the specified prefix.
        """

        # list will stop at specified delimiter, if '/' then not subfolder will be listed
        delimiter = '' if recursive else '/'
        paginator = self.s3client.get_paginator('list_objects_v2')

        try:
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter=delimiter)
        except ClientError as e:
            logging.error(e)
            return []

        object_list = []
        try:
            for page in pages:
                objects = list(map(lambda cont: (cont['Key'], cont['LastModified']), page['Contents']))
                object_list.extend(objects)
        except KeyError:
            logging.error(e)
            return []

        object_list.sort(reverse=order_desc, key=lambda x: x[1])

        return [obj[0] for obj in object_list]

    def list_folders(self, bucket, prefix: str = '', delimiter: str = '/'):
        """
        :param prefix: root path to list dir, if '' => list all dir directly under bucket
        """

        resp = self.s3client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter=delimiter)
        prefix_list = resp.get('CommonPrefixes')
        prefixs = []
        if prefix_list:
            for prefix in prefix_list:
                prefixs.append(prefix['Prefix'])

        prefixs.sort(reverse=False)
        return prefixs

    def head_objects(self, bucket, key):
        try:
            self.s3client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError:
            return False

    @staticmethod
    def clean_file_name(file_name: str):
        filename1 = re.sub(r'[:|<>?"\/\\\*\s\r\n]+', '_', file_name)[:128]
        filename2 = re.sub(r'[^A-Za-z0-9\_\-\~\s\.]+', '', filename1)
        return filename2