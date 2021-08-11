import pandas as pd

from tools.data_scanner import DataScanner
from tools.stat_tools import FitDistr

test_df = pd.read_csv('tests/test_data.csv')
fd = FitDistr()
# data_scanner = DataScanner('tests/test_json.json')
# # data_scanner.scan(test_df, 'test', 'v_2')
# # data_scanner.dump_to_json()
# test_df = test_df.append(test_df.astype(str))
# val = data_scanner.check_for_types_in_every_column(test_df)
# print(val)
fd.find_distribution(test_df['city_development_index'].values)
print(fd.dist_with_params)
print(fd.get_sample_from_distribution(10))
