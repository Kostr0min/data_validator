import numpy as np
import seaborn as sns
from fitter import Fitter
from scipy.stats import beta
from scipy.stats import burr
from scipy.stats import gamma
from scipy.stats import lognorm
from scipy.stats import norm


class FitDistr:

    def __init__(self):
        self.scipy_match_dict = {'lognorm': lognorm, 'beta': beta, 'burr': burr, 'norm': norm, 'gamma': gamma}
        self.dist_with_params = None

    @classmethod
    def _find_distribution(
        cls,
        data: np.array, summary: bool = True,
        valid_distr: set = (
            'gamma', 'lognorm',
            'beta', 'burr', 'norm',
        ),
    ):
        fit_dist = Fitter(
            data,
            distributions=list(valid_distr),
        )
        fit_dist.fit()
        if summary:
            fit_dist.summary()
        result_dist = fit_dist.get_best(method='sumsquare_error')

        return result_dist

    def find_distribution(
        self,
        data: np.array, summary: bool = True,
        valid_distr: set = (
            'gamma', 'lognorm',
            'beta', 'burr', 'norm',
        ),
    ):
        result_dist = self._find_distribution(data, summary=True, valid_distr=valid_distr)

        self.dist_with_params = result_dist

        return result_dist

    def plot_distplot(self):

        if self.dist_with_params:
            dist = self.scipy_match_dict[list(self.dist_with_params.keys())[0]]
            sns.distplot(dist.rvs(*list(self.dist_with_params.values())[0], size=1000))
        else:
            print('Specify distribution using find_distribution method')

    def get_sample_from_distribution(self, sample_size: int = 1000):
        if self.dist_with_params:
            dist = self.scipy_match_dict[list(self.dist_with_params.keys())[0]]
            return dist.rvs(*list(self.dist_with_params.values())[0], size=sample_size)

    @classmethod
    def bootstrapper(cls, array: np.array, n_bootstrap: int = 1000, conf_level: int = 95) -> dict:
        params_distribution = {'mean': [], 'median': []}
        for i in range(n_bootstrap):
            sampled = np.random.choice(array, array.shape[0], replace=True)
            params_distribution['mean'].append(np.mean(sampled))
            params_distribution['median'].append(np.median(sampled))
        left_b = round((1 - (conf_level / 100)) / 2 * n_bootstrap)
        right_b = round((1 + (conf_level / 100)) / 2 * n_bootstrap)
        for param in params_distribution.keys():
            params_distribution[param] = sorted(params_distribution[param])
        bootstrap_stat = {
            'mean_value': np.mean(params_distribution['mean']),
            'mean_value_ci': [
                params_distribution['mean'][left_b],
                params_distribution['mean'][right_b],
            ],
            'median_value': np.median(params_distribution['median']),
            'median_value_ci': [
                params_distribution['median'][left_b],
                params_distribution['median'][right_b],
            ],
        }

        return bootstrap_stat
