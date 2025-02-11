"""
This script is for downloading from Wasabi only for real time index performance records
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

temp_database_dir = os.path.join(os.getcwd(), "database", 'temp_database')


# noinspection PyShadowingNames
class DownloadSingleFileFromWasabi:
    """
    Download a file from Wasabi S3 bucket.
    By default, the file is downloaded from the path: index_v1/v1/simple/ticker_name/real_time/PT5S/
    """

    def __init__(self, index_ticker, testing_date, real_time, bucket_name='indices-backfill', print_log=True):
        self.s3 = boto3.client('s3',
                               endpoint_url='https://s3.us-east-2.wasabisys.com',
                               aws_access_key_id=aws_access_key_id,
                               aws_secret_access_key=aws_secret_access_key)

        self.bucket_name = bucket_name.lower()
        self.index_ticker = index_ticker.lower()
        self.real_time = real_time
        self.print_log = print_log

        # if the folder doesn't exist, create it
        if not os.path.exists(os.path.join(temp_database_dir, testing_date)):
            os.makedirs(os.path.join(temp_database_dir, testing_date))

    def download_file(self, date):
        if self.real_time:
            object_key = (f"index_v1/v1"
                          f"/extensive"
                          f"/{self.index_ticker}/real_time/PT5S"
                          f"/{self.index_ticker}_real_time_vwm_twap_100_{date}.csv.gz")

            local_file_path = os.path.join(temp_database_dir,
                                           date,
                                           f"{self.index_ticker}_real_time_vwm_twap_100_{date}.csv.gz")
        elif not self.real_time:
            date_without_day = datetime.datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m')
            object_key = (f'index_v1/v1/simple/{self.index_ticker}/index_fixing/PT24H/'
                          f'{self.index_ticker}_index_fixing_vwm_twap_100_{date_without_day}.csv.gz')
            local_file_path = os.path.join(temp_database_dir,
                                           f"{date}_fixing",
                                           f"{self.index_ticker}_index_fixing_vwm_twap_100_{date}.csv.gz")
        else:
            raise ValueError('real_time should be either True or False.')

        if self.print_log:
            print(f"Downloading {object_key} from {self.bucket_name} to {local_file_path}.")

        try:
            self.s3.download_file(self.bucket_name, object_key, local_file_path)

            with gzip.open(local_file_path, 'rb') as f_in:
                with open(local_file_path[:-3], 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

            if self.print_log:
                print(f"File downloaded to {local_file_path[:-3]} and decompressed.")

        except NoCredentialsError:
            print("AWS credentials not found or incorrect.")
        except Exception as e:
            print(f"An error occurred: {e}")

        # if it's index_fixing, only keep yesterday's data
        if not self.real_time:
            fixing_df = pd.read_csv(local_file_path[:-3])
            fixing_df['intervalEnd'] = pd.to_datetime(fixing_df['intervalEnd'])
            fixing_df = fixing_df[fixing_df['intervalEnd'].dt.date ==
                                  datetime.datetime.strptime(date, '%Y-%m-%d').date()]
            fixing_df.to_csv(local_file_path[:-3], index=False)
            print(f"File {local_file_path[:-3]} is updated to only keep yesterday's data.")

    def delete_file(self, date, gzip_file=True, csv_file=True):
        if self.real_time:
            local_file_path = os.path.join(temp_database_dir,
                                           date,
                                           f"{self.index_ticker}_real_time_vwm_twap_100_{date}.csv.gz")
        elif not self.real_time:
            local_file_path = os.path.join(temp_database_dir,
                                           f"{date}_fixing",
                                           f"{self.index_ticker}_index_fixing_vwm_twap_100_{date}.csv.gz")
        else:
            raise ValueError('Check delete file function')

        try:
            if gzip_file:
                os.remove(local_file_path)
        except:
            pass

        try:
            if csv_file:
                os.remove(local_file_path[:-3])
        except:
            pass

    def load_data_in_df(self, date, only_return_price_and_date=False):
        local_file_path = os.path.join(temp_database_dir,
                                       date,
                                       f"{self.index_ticker}_real_time_vwm_twap_100_{date}.csv.gz")
        try:
            data = pd.read_csv(local_file_path[:-3])
            data['intervalEnd'] = pd.to_datetime(data['intervalEnd'])

            if only_return_price_and_date:
                return data[['intervalStart', 'intervalEnd', 'price', 'parameters']]
            else:
                return data
        except FileNotFoundError:
            print(f"File {local_file_path[:-3]} not found.")
            return pd.DataFrame(columns=['intervalStart', 'intervalEnd', 'price', 'parameters'])


def single_wasabi_file_download(index_ticker, testing_date, bucket_name='indices-backfill', print_log=True):
    is_real_time = True
    local_file_path = None

    download_cli = DownloadSingleFileFromWasabi(index_ticker, testing_date, is_real_time, bucket_name, print_log)
    download_cli.download_file(date=testing_date)
    download_cli.delete_file(gzip_file=False, csv_file=False, date=testing_date)
    data = download_cli.load_data_in_df(only_return_price_and_date=True, date=testing_date)
    data.drop(columns=['intervalStart', 'price'], inplace=True)

    data['base_asset'] = data['parameters'].apply(lambda x: eval(x)[0]['asset'])
    data['exchanges'] = data['parameters'].apply(lambda x: eval(x)[0]['exchanges'])
    data['calc_window'] = data['parameters'].apply(lambda x: eval(x)[0]['calc_window'])
    data['partition_size'] = data['parameters'].apply(lambda x: eval(x)[0]['partition_size'])
    data.drop(columns=['parameters'], inplace=True)

    data.to_csv('test_wasabi_raw_data.csv', index=False)
    return data


if __name__ == '__main__':
    index_ticker = 'kk_rfr_btcusd'
    testing_date = '2025-01-03'
    bucket_name = 'indices-backfill'

    single_wasabi_file_download(index_ticker, testing_date, bucket_name=bucket_name, print_log=False)
