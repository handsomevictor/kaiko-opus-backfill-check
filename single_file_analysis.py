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

from resources import OPUS_URL


def get_opus_single_period(period):
    response = requests.get(
        OPUS_URL,
        params={
            "reviewName": "Q22020",
            "state": "REVIEW_STATE_UNKNOWN"
        },
        headers={
            "accept": "application/json"
        }
    )

    res = response.json()['items']
    res = pd.DataFrame(res)
    return res


if __name__ == '__main__':
    res = get_opus_single_period("Q22020")
    print(res)
    res.to_csv("opus_single_period.csv", index=False)


