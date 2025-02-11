import os
import datetime
import pandas as pd
from tqdm import tqdm

from get_wasabi_file import single_wasabi_file_download


def analyze_single_file(index_ticker, testing_date, bucket_name='indices-backfill', print_log=True):
    data = single_wasabi_file_download(index_ticker, testing_date, bucket_name=bucket_name, print_log=False)

    #


if __name__ == '__main__':
    index_ticker = 'kk_rfr_btcusd'
    testing_date = '2025-01-03'
    bucket_name = 'indices-backfill'
    analyze_single_file(index_ticker, testing_date, bucket_name=bucket_name)
