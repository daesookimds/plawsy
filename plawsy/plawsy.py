import json
import boto3
import pickle
import pandas as pd
from io import StringIO, BytesIO


class TakeAccessKey(object):
    def __init__(self, **param):
        param = {key: value for key, value in param.items()}
        self.aws_access_key_id = param['aws_access_key_id']
        self.aws_secret_access_key = param['aws_secret_access_key']
        self.region_name = param['region_name']
        self.session = boto3.Session(aws_access_key_id=self.aws_access_key_id,
                                     aws_secret_access_key=self.aws_secret_access_key, region_name=self.region_name)


class S3Connector(TakeAccessKey):
    def __init__(self, **param):
        super().__init__(**param)
        self.s3_client = self.session.client('s3')
        self.s3_resource = self.session.resource('s3')


    def get_bucket_list(self):
        self.buckets = [ins['Name'] for ins in self.s3_client.list_buckets()['Buckets']]

        return self.buckets


    def download(self, bucket_nm, remote_fn):
        '''
        download file from AWS S3
        :param bucket_nm: S3 bucket name you want to download
        :param remote_fn: S3 file path you want to download
        :return: download data
        '''
        file_nm = remote_fn.split('/')[-1]
        file_type = file_nm.split('.')[-1]
        content_obj = self.s3_resource.Object(bucket_nm, remote_fn).get()

        if file_type == 'json':
            content = content_obj['Body'].read().decode('utf-8')
            data = json.loads(content)
        elif file_type == 'csv':
            content = content_obj['Body'].read().decode('utf-8')
            data = pd.read_csv(StringIO(content))
        elif file_type == 'xlsx':
            content = content_obj['Body'].read()
            data = pd.read_excel(content)
        elif file_type == 'pkl':
            content = content_obj['Body'].read()
            data = pickle.loads(content)
        else:
            content = content_obj['Body'].read().decode('utf-8')
            data = content

        return data


    def upload(self, bucket_nm, remote_fn, upload_data):
        '''
        upload file to AWS S3
        :param data: A data you want to upload
        :param bucket_nm: S3 bucket name you want to upload
        :param remote_fn: S3 file path you want to upload
        :param type: Type of data you want to upload
        :return: None
        '''
        type = remote_fn.split('.')[-1]
        bucket = self.s3_resource.Bucket(bucket_nm)

        if type == 'json':
            bucket.Object(key=remote_fn).put(Body=json.dumps(upload_data))
        elif type == 'csv':
            buffer = StringIO()
            upload_data.to_csv(buffer, index=False)
            bucket.Object(key=remote_fn).put(Body=buffer.getvalue())
        elif type == 'xlsx':
            buffer = BytesIO()
            upload_data.to_excel(buffer, index=False)
            bucket.Object(key=remote_fn).put(Body=buffer.getvalue())
        elif type == 'pkl':
            bucket.Object(key=remote_fn).put(Body=pickle.dumps(upload_data))
        else:
            buffer = StringIO()
            buffer.write(upload_data)
            bucket.Object(key=remote_fn).put(Body=buffer.getvalue())

            