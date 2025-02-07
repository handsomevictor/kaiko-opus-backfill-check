"""
This file contains 2 functions:

1. download / update opus config for all periods
2. get config for a certain period
"""

import os
import json
import requests
import datetime
import pandas as pd
from tqdm import tqdm

from itertools import repeat
from concurrent.futures import ThreadPoolExecutor

from resources import OPUS_URL, START_YEAR, CURRENT_YEAR


def get_opus_single_period(period):
    response = requests.get(
        OPUS_URL,
        params={
            "reviewName": period,
            "state": "REVIEW_STATE_UNKNOWN"
        },
        headers={
            "accept": "application/json"
        }
    )

    res = response.json()['items']
    res = [i['rates'] for i in res]

    tmp_df = pd.DataFrame()
    for i in range(len(res)):
        tmp_df = pd.concat([tmp_df, pd.DataFrame(res[i])], axis=1)

    tmp_df = tmp_df.T
    tmp_df.index.name = "rate"

    tmp_df['period'] = period

    # iterate each row
    for idx, row in tmp_df.iterrows():
        row = row['commodityType']
        rate_type = ', '.join(row.keys())

        exchanges_included = row[rate_type]['request']['exchanges']
        exchanges_included = ','.join(exchanges_included)

        base_asset = row[rate_type]['request']['bases']['asset']

        calc_window = row[rate_type]['request']['interval']
        calc_window = int(calc_window.replace('s', ''))

        partition_size = row[rate_type]['request']['partitionSize']
        partition_size = int(partition_size.replace('s', ''))

        start_time = row[rate_type]['request']['startTime']
        end_time = row[rate_type]['request']['endTime']

        tmp_df.loc[idx, 'rate_type'] = rate_type
        tmp_df.loc[idx, 'exchanges_included'] = exchanges_included
        tmp_df.loc[idx, 'base_asset'] = base_asset
        tmp_df.loc[idx, 'calc_window'] = calc_window
        tmp_df.loc[idx, 'partition_size'] = partition_size
        tmp_df.loc[idx, 'start_time'] = pd.to_datetime(start_time)
        tmp_df.loc[idx, 'end_time'] = pd.to_datetime(end_time)

    try:
        tmp_df['calc_window'] = tmp_df['calc_window'].astype(int)
        tmp_df['partition_size'] = tmp_df['partition_size'].astype(int)
    except:
        print(f"Failed to get data for {period}, probably season doesn't exist")
        return

    tmp_df = tmp_df.drop(columns='commodityType')

    season = period[:2]
    year = period[2:]
    tmp_df.to_csv(os.path.join(os.getcwd(), 'database', 'opus_config', f"{year}_{season}.csv"), index=False)


def get_all_periods():
    start_year = START_YEAR
    current_year = CURRENT_YEAR

    seasons = ['Q1', 'Q2', 'Q3', 'Q4']
    years = list(range(start_year, current_year + 1))

    periods = []
    for year in years:
        for season in seasons:
            periods.append(f"{season}{year}")

    with ThreadPoolExecutor(max_workers=10) as executor:
        list(tqdm(executor.map(get_opus_single_period, periods), total=len(periods)))


if __name__ == '__main__':
    # get_opus_single_period("Q22020")
    get_all_periods()



