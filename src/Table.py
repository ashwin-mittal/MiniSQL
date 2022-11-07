# TESTED
import csv
import os
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup


class Table:
    @staticmethod
    def read_meta_data():
        file_name = os.path.join("files", "metadata.txt")
        with open(file_name, "r") as f:
            data = f.read()
            soup = BeautifulSoup(data, "html.parser")
            results = soup("begin_table")
        return [result.contents[0].split() for result in results]

    @staticmethod
    def read(name):
        try:
            file_name = os.path.join("files", f"{name}.csv")
            # read data in from file
            with open(file_name) as f:
                reader = csv.reader(f, quotechar='"', skipinitialspace=True)
                data = []
                for row in reader:
                    data.append([int(cell) for cell in row])
            cols_map = {}
            headers = Table.read_meta_data()
            table_header = None
            for header in headers:
                if header[0] == name:
                    table_header = [f"{col}" for col in header[1:]]
                    break
            for col in table_header:
                cols_map[col] = name
            return cols_map, pd.DataFrame(np.array(data), columns=table_header)
        except:
            raise Exception("table not found")
