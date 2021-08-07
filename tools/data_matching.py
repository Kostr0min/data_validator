import pandas as pd
from typing import List
import json
from tools.data_scanner import DataScanner


class DataChecker:

    def __init__(self, path_to_configure_json: str = None):
        # TODO: allow custom_check functions
        self.path_to_configure_json = path_to_configure_json
        self.worker = DataScanner()
        self.collected_configures = self.init_config_dict
        if not self.collected_configures or (len(self.collected_configures.keys()) == 0):
            print('Scan your data before check your data, smpl')

    @property
    def init_config_dict(self):
        """
        Returns
            loaded or created, if not exist, dict with dataframes configs
        -------
        """
        conf_dict = {}
        if self.path_to_configure_json:
            try:
                with open(self.path_to_configure_json, 'r') as f:
                    conf_dict = json.load(f)
            except FileNotFoundError:
                print(f'Invalid path to configuration json: {self.path_to_configure_json}')
        return conf_dict

    def extract_and_compare(self, *dataframes, data_names: dict = None, comparison_categories: List[str] = None):
        # All sheetcode ugly but valid code =)
        result_dict = {}
        if not data_names or data_names == {}:
            print('Please, specify data names')
        for i, df in enumerate(dataframes):
            name = list(data_names.keys())[i]
            df = df[data_names[name]] if len(data_names[name]) > 0 else df
            version = self.collected_configures[name]['last_version']
            valid_df_configuration = self.collected_configures[name][version]
            current_df_configuration = self.worker.scan(df, blank_shot=True)
            valid_df_configuration = {x: y for x, y in
                                      valid_df_configuration.items() if x in
                                      comparison_categories} if comparison_categories else valid_df_configuration

            current_df_configuration = {x: y for x, y in
                                        current_df_configuration.items() if x in
                                        comparison_categories} if comparison_categories else current_df_configuration

            result_dict[name] = valid_df_configuration == current_df_configuration
        return result_dict
