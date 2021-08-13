import typing
import pytest

import pandas as pd
import numpy as np

from tools.column_classifier import ColumnClassifier
from tests.conftest import test_data


@pytest.fixture()
def test_answers():
    return dict(numeric=['enrollee_id', 'city_development_index', 'training_hours', 'target'],
                datetime=['a', 'b'],
                id=[],
                category=(['city', 'city_development_index', 'gender',
                           'relevent_experience', 'enrolled_university',
                           'education_level', 'major_discipline',
                           'experience', 'company_size', 'company_type',
                           'last_new_job', 'training_hours', 'target'], ['enrollee_id']))


def test_numeric(test_data, test_answers):
    classifier = ColumnClassifier()
    data = test_data
    answers = test_answers.get('numeric', None)
    assert isinstance(data, pd.DataFrame)
    assert isinstance(answers, typing.List)

    numeric = classifier.extract_numeric(data=data)
    assert isinstance(numeric, typing.List)
    for obj in numeric:
        assert isinstance(obj, str)

    assert set(numeric) == set(answers)


def test_datetime(test_data, test_answers):
    classifier = ColumnClassifier()
    data = test_data
    answers = test_answers.get('datetime', None)
    assert isinstance(data, pd.DataFrame)
    assert isinstance(answers, typing.List)

    _datetime = classifier.extract_datetime(data)
    assert isinstance(_datetime, typing.List)
    # После добавления дэйттаймов в ксв уберем
    _data = pd.DataFrame({'a': pd.date_range(pd.Timestamp('today'), periods=10),
                          'b': pd.date_range(pd.Timestamp('today'), periods=10, freq='D').strftime('%Y/%m/%d'),
                          'c': range(10),
                          'd': ['d'] * 10, })
    _datetime = classifier.extract_datetime(_data)
    for obj in _datetime:
        assert isinstance(obj, str)

    assert set(_datetime) == set(answers)


def test_id(test_data, test_answers):
    classifier = ColumnClassifier()
    data = test_data
    answers = test_answers.get('id', None)
    numeric = test_answers.get('numeric', None)
    assert isinstance(data, pd.DataFrame)
    assert isinstance(answers, typing.List)
    assert isinstance(numeric, typing.List)

    _id, _numeric = classifier.extract_id(data, numeric_columns=numeric)
    assert isinstance(_id, typing.List)
    assert _id == ['enrollee_id']
    assert isinstance(_numeric, typing.List)
    assert set(_numeric) == set(numeric)
    # После добавления id в ксв уберем
    _data = pd.DataFrame({'a': range(10),
                          'b': np.arange(10) / 100})
    _id, _numeric = classifier.extract_id(_data, _data.columns.to_list())
    assert _id == ['a']
    assert _numeric == ['b']


def test_category(test_data, test_answers):
    classifier = ColumnClassifier()
    data = test_data
    answer_1, answer_2 = test_answers.get('category', None)
    numeric = test_answers.get('numeric', None)
    assert isinstance(data, pd.DataFrame)
    assert isinstance(answer_1, typing.List)
    assert isinstance(answer_2, typing.List)
    assert isinstance(numeric, typing.List)

    category, _numeric = classifier.extract_category(data, numeric_columns=numeric, threshold=0.2)
    assert isinstance(category, typing.List)
    assert isinstance(_numeric, typing.List)
    for obj in category:
        assert isinstance(obj, str)
    assert set(category) == set(answer_1)
    assert set(_numeric) == set(answer_2)


def test_schema(test_data):
    classifier = ColumnClassifier()
    data = test_data

    schema = classifier.schema(data, data_name='test', to_drop=['target'])
    assert isinstance(schema, typing.Dict)
    assert 'test' in [*classifier.schema_container]
    assert len(schema.get('test', [])) == 6
    keys = ['numeric', 'datetime', 'id', 'category', 'object', 'not_used']
    schema_keys = [*schema.get('test', None)]
    assert set(keys) == set(schema_keys)
