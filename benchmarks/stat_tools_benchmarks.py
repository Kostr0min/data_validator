import numpy as np
import pandas as pd
from test_tools import time_it

from tools.stat_tools import FitDistr

test_df = pd.read_csv('../tests/test_data.csv')
fd = FitDistr()
print(np.mean(test_df['city_development_index'].values), np.median(test_df['city_development_index'].values))


@time_it
def single_core():
    print(FitDistr.bootstrapper(array=test_df['city_development_index'].values, n_bootstrap=10000))


@time_it
def multicore():
    print(FitDistr.bootstrapper_(array=test_df['city_development_index'].values, n_bootstrap=10000))


single_core()
multicore()
