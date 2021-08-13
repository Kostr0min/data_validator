import os

import pandas as pd
import pytest

from tools.stat_tools import FitDistr


def test_find_distribution(test_data):

    fd = FitDistr()
    data = test_data['city_development_index'].values
    fd.find_distribution(data)

    assert 'burr' in fd.dist_with_params.keys()
