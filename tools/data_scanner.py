import pandas as pd
from typing import List
import numpy as np
import json


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
                with open(self.path_to_conf_json, 'r') as f:
                    conf_dict = json.load(f)
            except FileNotFoundError:
                print(f'Ignore this warning if new json. \n'
                      f'Invalid path to config json: {self.path_to_conf_json}')
        return conf_dict

    def dump_to_json(self, path: str = None):
        path_to_config = path if path else self.path_to_conf_json
        try:
            with open(path_to_config, 'w') as f:
                json.dump(self.collected_configs, f, indent=4)
        except FileNotFoundError:
            print(f'Invalid path to configuraton json: {path_to_config}')

    @staticmethod
    def statistics_extractor(dataframe: pd.DataFrame, numeric_columns: List[str] = None):
        """
        Simple pandas describe data extractor
        numeric_columns: list of numeric columns names
        """
        columns_list = numeric_columns if numeric_columns else dataframe.select_dtypes(include=np.number).columns
        return dataframe[columns_list].describe().to_dict()

    def scan(self, dataframe: pd.DataFrame, name: str = None, version: str = None, blank_shot: bool = False):
        """
        Base json-element creator. Collect all data by key [version] for dataframe key [name] in
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
        config_dict = {'dtypes_': dataframe.dtypes.apply(lambda x: x.name).to_dict(),
                       'columns_': dataframe.columns.to_list(), 'shape_': list(dataframe.shape),
                       'numeric_columns_stats': self.statistics_extractor(dataframe)}
        if blank_shot:
            return config_dict
        if name not in self.collected_configs.keys():
            self.collected_configs[name] = {}
        self.collected_configs[name][version] = config_dict
        self.collected_configs[name]['last_version'] = version

