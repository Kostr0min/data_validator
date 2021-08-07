import pandas as pd

from tools.data_scanner import DataScanner

test_df = pd.read_csv('tests/test_data.csv')
data_scanner = DataScanner('tests/test_json.json')
# data_scanner.scan(test_df, 'test', 'v_2')
# data_scanner.dump_to_json()
test_df = test_df.append(test_df.astype(str))
val = data_scanner.check_for_types_in_every_column(test_df)
print(val)
