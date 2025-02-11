"""
This script is used for getting all file names (or appointed file names) from Wasabi S3 bucket.
"""

import os
import gzip
import boto3
import shutil
import datetime
import pandas as pd
from botocore.exceptions import NoCredentialsError

aws_access_key_id = os.environ.get("ANASTASIA_WASABI_ACCESS_KEY")
aws_secret_access_key = os.environ.get("ANASTASIA_WASABI_SECRET_KEY")


def list_all_files_under_index_code(bucket_name, index_code):
    s3 = boto3.client('s3',
                      endpoint_url='https://s3.us-east-2.wasabisys.com',
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)

    files = []
    continuation_token = None

    while True:
        list_params = {
            'Bucket': bucket_name,
            'Prefix': f'index_v1/v1/extensive/{index_code}/',  # 只获取该文件夹下的内容
            'MaxKeys': 1000
        }
        if continuation_token:
            list_params['ContinuationToken'] = continuation_token

        response = s3.list_objects_v2(**list_params)

        if 'Contents' in response:
            files.extend([obj['Key'] for obj in response['Contents']])

        if response.get('IsTruncated'):
            continuation_token = response['NextContinuationToken']
        else:
            break

    files = [file for file in files if file.endswith('.csv.gz')]

    # exclude the first file
    file_dates = [file.split('/')[-1].split('_')[-1].split('.')[0] for file in files][1:]
    file_dates.sort()

    return file_dates


def get_all_dates_for_index_code(bucket_name, index_code):
    return list_all_files_under_index_code(bucket_name, index_code)


if __name__ == '__main__':
    bucket_name = 'indices-backfill'
    index_code = 'kk_rfr_btcusd'
    file_list = list_all_files_under_index_code(bucket_name, index_code)

    # save to txt file
    with open('all_wasabi_file_dates.txt', 'w') as f:
        for file in file_list:
            f.write(f"{file}\n")
