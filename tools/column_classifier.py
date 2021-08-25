import typing

import numpy as np
import pandas as pd
from dateutil.parser import ParserError
from pandas.api.types import CategoricalDtype
from pandas.errors import OutOfBoundsDatetime


class ColumnClassifier:
    def __init__(self):
        self.schema_container: typing.Dict[str, typing.Dict[str, typing.List]] = {}

    def schema(
            self,
            data: pd.DataFrame,
            data_name: str = 'init',
            columns_to_drop: typing.Union[typing.List, None] = None,
    ) -> typing.Dict[str, typing.Dict[str, typing.List]]:
        """
        Update schema_container, which contains all data columns types.
        Based on classmethod 'classify_columns'.

        Parameters
        ----------
        data
            pd.DataFrame: dataframe.
        data_name
            str: name for schema_container.
        columns_to_drop
            list: contains a list of columns to drop.

        Returns
        -------
        self.schema_container
            All available schemas of dataframes.
        Examples
        -------
        >>> from tools.column_classifier import ColumnClassifier
        >>> cc = ColumnClassifier()
        >>> cc.schema(data=pd.read_csv('./tests/test_data.csv'),
        ...           data_name='test',
        ...           columns_to_drop=['target'])
        """
        self.schema_container = {
            **self.schema_container, **self.classify_columns(
                data=data,
                data_name=data_name,
                columns_to_drop=columns_to_drop,
            ),
        }

        return self.schema_container

    @classmethod
    def classify_columns(
            cls, data: pd.DataFrame, data_name: str = 'init',
            columns_to_drop: typing.Union[typing.List, None] = None,
    ) -> typing.Dict[str, typing.Dict[str, typing.List]]:
        """
        Main pipeline to classifier columns into next categories:
            - numeric,
            - datetime,
            - id,
            - category,
            - object,
            - not used.
        Parameters
        ----------
        data
            pd.DataFrame: dataframe.
        data_name
            str: name for schema_container.
        columns_to_drop
            list: contains a list of columns to drop.

        Returns
        -------
        dict
            A dict with classified columns.

        Examples
        -------
        >>> from tools.column_classifier import ColumnClassifier
        >>> ColumnClassifier.classify_columns(data=pd.read_csv('./tests/test_data.csv'),
        ...                                   data_name='test',
        ...                                   columns_to_drop=['target'])
        """
        data_cut = data.drop(columns=columns_to_drop) if columns_to_drop else data

        _numeric = cls.extract_numeric(data=data_cut)
        _datetime = cls.extract_datetime(data=data_cut)
        _id, _numeric = cls.extract_id(data=data_cut, numeric_columns=_numeric)
        _category, _numeric = cls.extract_category(data_cut, numeric_columns=_numeric, threshold=0.2)
        _object = list(
            set(data_cut.columns.to_list()) -
            set(_id + _datetime + _category + _numeric),
        )

        return {
            data_name: dict(
                numeric=_numeric,
                datetime=_datetime,
                id=_id,
                category=_category,
                object=_object,
                not_used=columns_to_drop,
            ),
        }

    @staticmethod
    def extract_numeric(data: pd.DataFrame) -> typing.List[str]:
        """
        Method to extract numeric columns.

        Parameters
        ----------
        data
            pd.DataFrame: dataframe.

        Returns
        -------
        list
            A list with possible numeric columns.
        """
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
        """
        Method to extract datetime columns.

        Parameters
        ----------
        data
            pd.DataFrame: dataframe.

        Returns
        -------
        list
            A list with possible datetime columns.
        """
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
        """
        Method to extract id columns.

        Parameters
        ----------
        data
            pd.DataFrame: dataframe.
        numeric_columns
            list: A list with numeric columns.

        Returns
        -------
        tuple
            A list with possible id columns and numeric columns with removed ones.
        """
        id_columns: typing.List[str] = []
        for column in numeric_columns:
            values = data.loc[:, column]
            if sum(np.abs(values.diff()[1:] - 1) < 1e-7) & (values.nunique() == values.size):
                id_columns.append(column)
                numeric_columns.remove(column)
        return id_columns, numeric_columns

    @staticmethod
    def extract_category(
            data: pd.DataFrame,
            numeric_columns: typing.List[str],
            threshold: float = 0.2,
    ) -> (typing.List[str], typing.List[str]):
        """
        Method to extract category columns.

        Parameters
        ----------
        data
            pd.DataFrame: dataframe.
        numeric_columns
            list: A list with numeric columns.
        threshold
            int: threshold of percent unique values. Default 0.2.

        Returns
        -------
        tuple
            A list with possible category columns and numeric columns with removed ones.
        """
        category_columns: typing.List[str] = []
        for column in data.columns:
            interim: pd.Series = data.loc[:, column]
            if interim.nunique() / interim.size <= threshold:
                category_columns.append(column)
                if column in numeric_columns:
                    numeric_columns.remove(column)
        return category_columns, numeric_columns

    def apply_schema(
        self,
        data: pd.DataFrame,
        data_name: str = None,
        schema_types: typing.Optional[dict] = None,
        columns_to_drop: typing.Optional[list] = None,
    ) -> pd.DataFrame:
        """
        Pipeline to apply extract column types to dataframe.
        Will apply next dtypes:
            - numeric: np.number,
            - datetime: np.datetime64,
            - category: pd.api.types.CategoricalDtype()
        Parameters
        ----------
        data
            pd.DataFrame: dataframe.
        data_name
            str: name for schema_container. Default None.
            If None will create new schema, else get from schema_container.
        schema_types
            dict: schema dict to apply. Default None.
            If None will create new schema by deafult name = 'inhouse'
        columns_to_drop
            list: a list of columns to drop.

        Returns
        -------
        pd.DataFrame
            A dataframe with applied numpy and pandas dtypes

        Examples
        -------
        >>> from tools.column_classifier import ColumnClassifier
        >>> cc = ColumnClassifier()
        >>> new_data = cc.apply_schema(data)
        """
        inhouse_name = 'inhouse'
        dtype_dict = {
            'numeric': np.number,
            'datetime': np.datetime64,
            'category': CategoricalDtype(),
        }

        if not schema_types:
            if not data_name:
                self.schema(data=data, data_name=inhouse_name, columns_to_drop=columns_to_drop)
                data_name = inhouse_name
            schema_types = self.schema_container.get(data_name, None)

        if not schema_types:
            raise NotImplementedError('Need schema to apply to dataframe')

        data_inplace = data.copy()

        for column_type, column_list in schema_types.items():
            if isinstance(column_list, list):
                if not len(column_list) == 0:
                    data_inplace.loc[:, column_list] = data_inplace.loc[:, column_list].astype(
                        dtype=dtype_dict.get(column_type, str),
                    )

        return data_inplace
