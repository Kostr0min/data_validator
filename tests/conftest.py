import os

import pandas as pd
import pytest


@pytest.fixture()
def test_data():
    return pd.read_csv('tests/test_data.csv')


@pytest.fixture()
def check_csv_exist_and_remove():

    assert os.path.isfile('tests/test_data.csv')

    yield

    os.remove('tests/test_json.json')
