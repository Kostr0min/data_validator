import pandas as pd
import numpy as np
import typing


class ColumnClassifier:
    def __init__(self):
        self.schema_container: typing.Dict[typing.AnyStr, typing.AnyStr] = {}
        self._numeric: typing.List[typing.AnyStr] = []
        self._datetime: typing.List[typing.AnyStr] = []
        self._category: typing.List[typing.AnyStr] = []
        self._id: typing.List[typing.AnyStr] = []

    def schema(self,
               data: pd.DataFrame,
               data_name: typing.AnyStr = 'init',
               to_drop: typing.Union[typing.List, None] = None) -> typing.Dict[typing.AnyStr, typing.AnyStr]:
        self.__delete__()

        data_cut: pd.DataFrame = data.drop(columns=to_drop) if to_drop else data
        # Попахивает
        self.extract_numeric(data_cut)
        self.extract_datetime(data_cut)
        self.extract_id(data_cut)
        self.extract_category(data_cut, 0.2)
        _object = list(set(data_cut.columns.to_list()) -
                       set(self._id + self._datetime + self._category + self._numeric))

        self.schema_container.update({data_name: dict(numeric=self._numeric,
                                                      datetime=self._datetime,
                                                      id=self._id,
                                                      category=self._category,
                                                      object=_object)})

        return self.schema_container

    def extract_numeric(self, data: pd.DataFrame) -> typing.NoReturn:
        numeric_columns: typing.List[typing.AnyStr] = [column for column in data.select_dtypes(include=np.number)]
        for column in data.select_dtypes(include=[np.object_, 'category']).columns:
            try:
                data.loc[:, column].astype(dtype=np.number)
                numeric_columns.append(column)
            except ValueError:
                continue
        self._numeric.extend(numeric_columns)

    def extract_datetime(self, data: pd.DataFrame) -> typing.NoReturn:
        datetime_columns: typing.List[typing.AnyStr] = [column for column in data.select_dtypes(include=np.datetime64)]
        for column in data.select_dtypes(include=[np.object_, 'category']).columns:
            try:
                data.loc[:, column].astype(dtype=np.datetime64)
                datetime_columns.append(column)
            except (TypeError, ValueError):
                continue
        self._datetime.extend(datetime_columns)

    def extract_id(self, data: pd.DataFrame) -> typing.NoReturn:
        for column in self._numeric:
            values = data.loc[:, column]
            if sum(np.abs(values.diff()[1:] - 1) < 1e-7) & (values.nunique() == values.size):
                self._id.append(column)
                if column in self._numeric:
                    self._numeric.remove(column)

    def extract_category(self, data: pd.DataFrame, threshold: float = 0.2) -> typing.NoReturn:
        for column in data.columns:
            interim: pd.Series = data.loc[:, column]
            if interim.nunique() / interim.size <= threshold:
                self._category.append(column)
                if column in self._numeric:
                    self._numeric.remove(column)

    def __delete__(self):
        self._id.clear()
        self._numeric.clear()
        self._category.clear()
        self._datetime.clear()
