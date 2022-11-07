# TESTED
import numpy as np
from tabulate import tabulate


class Print:
    @staticmethod
    def print(query, results):
        # for debegging purposes
        # print(query)
        # print(results.columns.values, np.asarray(results))
        print(tabulate(results, headers="keys", tablefmt="psql"))
        # print(",".join(results.columns.values))
        # for row in np.asarray(results):
            # print(",".join(str(val) for val in row))
