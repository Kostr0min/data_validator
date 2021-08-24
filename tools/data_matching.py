import json
from json import JSONDecodeError
from typing import List

import pandas as pd

from tools.data_scanner import DataScanner


class DataMatcher:

    def __init__(self, original_frame: pd.DataFrame = None, path_to_json: str = None):
        if original_frame and (not isinstance(original_frame, pd.DataFrame)):
            raise ValueError(
                'Original frame must be a pandas DataFrame',
            )

        self.path_to_json = path_to_json
        self.original_frame = original_frame if original_frame else None
        self.original_config = self.init_config_dict(path_to_json) if path_to_json else None
        if (not self.original_config) and (not self.original_frame):
            raise Exception('The Matcher needs a original dataframe or path to config json')
        # self.fresh_frame = fresh_frame

    def extractor(self, *dataframes, data_names: dict = None, comparison_categories: List[str] = None):
        data_scanner = DataScanner(self.path_to_json)
        result_dict = {}
        if not self.path_to_json:
            data_scanner.scan(self.original_frame, 'original', 'v_1')
        for i, df in enumerate(dataframes):
            name = list(data_names.keys())[i]
            df = df[data_names[name]] if len(data_names[name]) > 0 else df
            version = self.original_config[name]['last_version']
            valid_df_configuration = self.original_config[name][version]
            current_df_configuration = data_scanner.scan(df, blank_shot=True)
            valid_df_configuration = {
                x: y for x, y in
                valid_df_configuration.items() if x in
                comparison_categories
            } if comparison_categories else valid_df_configuration

            current_df_configuration = {
                x: y for x, y in
                current_df_configuration.items() if x in
                comparison_categories
            } if comparison_categories else current_df_configuration

            result_dict[name] = valid_df_configuration == current_df_configuration
        return result_dict

    # def extractor(self):
    #     data_scanner = DataScanner(self.path_to_json)
    #     if not self.path_to_json:
    #         data_scanner.scan(self.original_frame, 'original', 'v_1')
    #
    #     original_config = data_scanner.collected_configs
    #     data_scanner.scan(self.fresh_frame)
    #     fresh_config = data_scanner.collected_configs
    #
    #     return original_config, fresh_config

    @staticmethod
    def init_config_dict(path_to_conf_json):
        """
        Returns
            loaded or created dict with dataframes configs
        -------
        """
        conf_dict = {}
        if path_to_conf_json:
            try:
                with open(path_to_conf_json) as f:
                    conf_dict = json.load(f)
            except (FileNotFoundError, JSONDecodeError):
                print(
                    f'Ignore this warning if new json. \n'
                    f'Invalid path to config json: {path_to_conf_json}',
                )
        return conf_dict


# class DataChecker:
#
#     def __init__(self, path_to_configure_json: str = None):
#         # TODO: allow custom_check functions
#         self.path_to_configure_json = path_to_configure_json
#         self.worker = DataScanner()
#         self.collected_configures = self.init_config_dict
#         if not self.collected_configures or (len(self.collected_configures.keys()) == 0):
#             print('Scan your data before check your data, smpl')
#
#     @property
#     def init_config_dict(self):
#         """
#         Returns
#             loaded or created, if not exist, dict with dataframes configs
#         -------
#         """
#         conf_dict = {}
#         if self.path_to_configure_json:
#             try:
#                 with open(self.path_to_configure_json) as f:
#                     conf_dict = json.load(f)
#             except FileNotFoundError:
#                 print(
#                     f'Invalid path to configuration json: {self.path_to_configure_json}',
#                 )
#         return conf_dict
#
#     def extract_and_compare(self, *dataframes, data_names: dict = None, comparison_categories: List[str] = None):
#         # All sheetcode ugly but valid code =)
#         result_dict = {}
#         if not data_names or data_names == {}:
#             print('Please, specify data names')
#         for i, df in enumerate(dataframes):
#             name = list(data_names.keys())[i]
#             df = df[data_names[name]] if len(data_names[name]) > 0 else df
#             version = self.collected_configures[name]['last_version']
#             valid_df_configuration = self.collected_configures[name][version]
#             current_df_configuration = self.worker.scan(df, blank_shot=True)
#             valid_df_configuration = {
#                 x: y for x, y in
#                 valid_df_configuration.items() if x in
#                 comparison_categories
#             } if comparison_categories else valid_df_configuration
#
#             current_df_configuration = {
#                 x: y for x, y in
#                 current_df_configuration.items() if x in
#                 comparison_categories
#             } if comparison_categories else current_df_configuration
#
#             result_dict[name] = valid_df_configuration == current_df_configuration
#         return result_dict
