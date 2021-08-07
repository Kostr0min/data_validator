import os

import pandas as pd
import pytest

from tools.data_scanner import DataScanner


# THX for the test data file: https://www.kaggle.com/arashnic/hr-analytics-job-change-of-data-scientists


@pytest.fixture()
def data_for_check_types():
    test_df = pd.read_csv('tests/test_data.csv')
    test_df = test_df.append(test_df.astype(str))

    return test_df


@pytest.fixture()
def data_for_dump():

    test_df = pd.read_csv('tests/test_data.csv')
    data_scanner = DataScanner('tests/test_json.json')
    data_scanner.scan(test_df, 'test', 'v_1')

    return data_scanner


def test_datascanner_scan(test_data, path: str = './test_json.json'):
    data_scanner = DataScanner()

    data_scanner.scan(test_data, 'test', 'v_1')

    assert 'test' in data_scanner.collected_configs
    assert 'v_1' in data_scanner.collected_configs['test']
    assert data_scanner.collected_configs['test']['last_version'] == 'v_1'
    # А вот это уже попахивает:
    assert list(data_scanner.collected_configs['test']['v_1'].keys()) == [
        'dtypes_', 'columns_', 'shape_',
        'numeric_columns_stats', 'unique_types',
    ]


def test_datascanner_dump_to_json(data_for_dump, check_csv_exist_and_remove):
    data_for_dump.dump_to_json()

    assert os.path.isfile('tests/test_json.json')


def test_datascanner_check_for_types_in_every_column(data_for_check_types):
    data_scanner = DataScanner()
    val = data_scanner.check_for_types_in_every_column(data_for_check_types)

    assert val == {
        'enrollee_id': ['int', 'str'], 'city': ['str'], 'city_development_index': ['float', 'str'],
        'gender': ['str', 'float'], 'relevent_experience': ['str'], 'enrolled_university': ['str', 'float'],
        'education_level': ['str', 'float'], 'major_discipline': ['str', 'float'],
        'experience': ['str', 'float'], 'company_size': ['float', 'str'], 'company_type': ['float', 'str'],
        'last_new_job': ['str', 'float'], 'training_hours': ['int', 'str'], 'target': ['float', 'str'],
    }
