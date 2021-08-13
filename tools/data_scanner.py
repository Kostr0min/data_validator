import json
from json import JSONDecodeError
from typing import List

import numpy as np
import pandas as pd

from tools.stat_tools import FitDistr


class DataScanner:
    """
    .scan() dataframes
    .dump_to_json(path) dump all scanned dataframe configs to path
    """

    def __init__(self, path_to_conf_json: str = None):
        self.path_to_conf_json = path_to_conf_json
        self.collected_configs = self.init_config_dict

    @property
    def init_config_dict(self):
        """
        Returns
            loaded or created dict with dataframes configs
        -------
        """
        conf_dict = {}
        if self.path_to_conf_json:
            try:
                with open(self.path_to_conf_json) as f:
                    conf_dict = json.load(f)
            except (FileNotFoundError, JSONDecodeError):
                print(
                    f'Ignore this warning if new json. \n'
                    f'Invalid path to config json: {self.path_to_conf_json}',
                )
        return conf_dict

    def dump_to_json(self, path: str = None):
        path_to_config = path if path else self.path_to_conf_json
        try:
            with open(path_to_config, 'w') as f:
                json.dump(self.collected_configs, f, indent=4)
        except FileNotFoundError:
            print(f'Invalid path to configuraton json: {path_to_config}')

    @staticmethod
    def check_for_types_in_every_column(dataframe: pd.DataFrame):
        """
        Check all columns values by type() and store unique types with column key

        govnocode edition

        :param dataframe:
        :return:
        """
        types_dict = {}
        for col in dataframe.columns:
            val = dataframe[col].apply(lambda x: type(x)).unique()
            types_dict[col] = [str(x)[8:-2] for x in val]
            if len(val) > 1:
                print(
                    f'WARNING, NOT ALL VALUES ARE OF THE SAME TYPE. COLUMN {col}\n',
                    f'TYPES {val}',
                )
        return types_dict

    @staticmethod
    def statistics_extractor(dataframe: pd.DataFrame, numeric_columns: List[str] = None) -> dict:
        """
        Simple pandas describe data extractor
        numeric_columns: list of numeric columns names
        """
        columns_list = numeric_columns if numeric_columns else dataframe.select_dtypes(
            include=np.number,
        ).columns
        return dataframe[columns_list].describe().to_dict()

    @staticmethod
    def bootstrap_stats_extractor(dataframe: pd.DataFrame, numeric_columns: List[str]):
        bootstrap_stats_result = {}
        for num_col in numeric_columns:
            stats = FitDistr.bootstrapper(dataframe[num_col].values)
            bootstrap_stats_result[num_col] = stats
        return bootstrap_stats_result

    def scan(self, dataframe: pd.DataFrame, name: str = None, version: str = None, blank_shot: bool = False):
        """
        Base json-element creator using extractor methods. Collect all data by key [version] for dataframe key [name] in
        self.collected_configures
        Parameters
        ----------
        dataframe
        name
        version
        blank_shot:
        Returns
        -------
         Examples
        --------
        {examples}
        >>> data_scanner = DataScanner()
        >>> df = pd.DataFrame({'integ': [4, 2],
        ...                     'strgs': ["Gilfoyle's", "beard"]})
        >>> data_scanner.scan(df, 'digits_and_words', 'version_1')

        """
        # TODO: add data, extracted using column classificator and make a copy of dataframe with new classified types
        config_dict = {
            'dtypes_': dataframe.dtypes.apply(lambda x: x.name).to_dict(),
            'columns_': dataframe.columns.to_list(), 'shape_': list(dataframe.shape),
            'numeric_columns_stats': self.statistics_extractor(dataframe),
            'unique_types': self.check_for_types_in_every_column(dataframe),
            'bootstrap_stats': self.bootstrap_stats_extractor(
                dataframe,
                numeric_columns=dataframe.select_dtypes(
                    include=np.number,
                ).columns.tolist(),
            ),
        }
        if blank_shot:
            return config_dict
        if name not in self.collected_configs.keys():
            self.collected_configs[name] = {}
        self.collected_configs[name][version] = config_dict
        self.collected_configs[name]['last_version'] = version
