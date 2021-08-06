import pandas as pd
import pytest
import os
from tools.data_scanner import DataScanner


""" THX for the test data file: https://www.kaggle.com/arashnic/hr-analytics-job-change-of-data-scientists"""


@pytest.fixture()
def test_data():
    return pd.read_csv("tests/test_data.csv")


@pytest.fixture()
def data_for_dump():

    test_df = pd.read_csv("tests/test_data.csv")
    data_scanner = DataScanner("tests/test_json.json")
    data_scanner.scan(test_df, 'test', 'v_1')

    return data_scanner


@pytest.fixture()
def check_csv_exist_and_remove():

    assert os.path.isfile("tests/test_data.csv")

    yield

    os.remove("tests/test_json.json")


def test_datascanner_scan(test_data, path: str = "./test_json.json"):
    data_scanner = DataScanner()

    data_scanner.scan(test_data, 'test', 'v_1')

    assert 'test' in data_scanner.collected_configs
    assert 'v_1' in data_scanner.collected_configs['test']
    assert data_scanner.collected_configs['test']['last_version'] == 'v_1'
    # А вот это уже попахивает:
    assert list(data_scanner.collected_configs['test']['v_1'].keys()) == ["dtypes_", "columns_", "shape_",
                                                                          "numeric_columns_stats"]


def test_datascanner_dump_to_json(data_for_dump, check_csv_exist_and_remove):
    data_for_dump.dump_to_json()

    assert os.path.isfile("tests/test_json.json")
