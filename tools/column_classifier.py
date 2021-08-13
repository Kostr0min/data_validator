import pandas as pd
import numpy as np
import typing
from pandas.errors import OutOfBoundsDatetime
from dateutil.parser import ParserError


class ColumnClassifier:
    def __init__(self):
        self.schema_container: typing.Dict[str, typing.Dict[str, typing.List]] = {}

    def schema(self,
               data: pd.DataFrame,
               data_name: str = 'init',
               to_drop: typing.Union[typing.List, None] = None) -> typing.Dict[str, typing.Dict[str, typing.List]]:
        data_cut: pd.DataFrame = data.drop(columns=to_drop) if to_drop else data

        _numeric = self.extract_numeric(data_cut)
        _datetime = self.extract_datetime(data_cut)
        _id, _numeric = self.extract_id(data_cut, numeric_columns=_numeric)
        _category, _numeric = self.extract_category(data_cut, numeric_columns=_numeric, threshold=0.2)
        _object = list(set(data_cut.columns.to_list()) -
                       set(_id + _datetime + _category + _numeric))

        self.schema_container.update({data_name: dict(numeric=_numeric,
                                                      datetime=_datetime,
                                                      id=_id,
                                                      category=_category,
                                                      object=_object,
                                                      not_used=to_drop)})

        return self.schema_container

    @staticmethod
    def extract_numeric(data: pd.DataFrame) -> typing.List[str]:
        numeric_columns: typing.List[str] = [column for column in data.select_dtypes(include=np.number)]
        for column in data.select_dtypes(include=[np.object_, 'category']).columns:
            try:
                data.loc[:, column].astype(dtype=np.number)
                numeric_columns.append(column)
            except ValueError:
                continue
        return numeric_columns

    @staticmethod
    def extract_datetime(data: pd.DataFrame) -> typing.List[str]:
        datetime_columns: typing.List[str] = [column for column in data.select_dtypes(include=np.datetime64)]
        for column in data.select_dtypes(include=[np.object_, 'category']).columns:
            try:
                data.loc[:, column].astype(dtype=np.datetime64)
                datetime_columns.append(column)
            except (TypeError, ValueError, ParserError, OutOfBoundsDatetime):
                continue
        return datetime_columns

    @staticmethod
    def extract_id(data: pd.DataFrame, numeric_columns: typing.List[str]) -> (typing.List[str], typing.List[str]):
        id_columns: typing.List[str] = []
        for column in numeric_columns:
            values = data.loc[:, column]
            if sum(np.abs(values.diff()[1:] - 1) < 1e-7) & (values.nunique() == values.size):
                id_columns.append(column)
                numeric_columns.remove(column)
        return id_columns, numeric_columns

    @staticmethod
    def extract_category(data: pd.DataFrame,
                         numeric_columns: typing.List[str],
                         threshold: float = 0.2) -> (typing.List[str], typing.List[str]):
        category_columns: typing.List[str] = []
        for column in data.columns:
            interim: pd.Series = data.loc[:, column]
            if interim.nunique() / interim.size <= threshold:
                category_columns.append(column)
                if column in numeric_columns:
                    numeric_columns.remove(column)
        return category_columns, numeric_columns
