# TESTED
import pandas as pd
import pandas as pd
from Table import Table


class Handler:
    @staticmethod
    def handle_from(query):
        tables = query["FROM"]
        if len(tables) < 1:
            print("The number of tables should be greater than one.")
            raise Exception()
        cols_map = {}
        map, results = Table.read(tables[0])
        cols_map.update(map)
        for index in range(1, len(tables)):
            map, data = Table.read(tables[index])
            results = pd.merge(results, data, how="cross")
            cols_map.update(map)
        return cols_map, results

    @staticmethod
    def handle_select(cols_map, results, query):
        attrs = query["SELECT"]
        if attrs == None:
            attrs = list(results.columns.values)

        if len(attrs) == 0:
            print("The number of attributes to select should be greater than one.")
            raise Exception()

        fun_attrs = {}
        non_fun_attrs = []

        for cur_attr in attrs:
            if isinstance(cur_attr, tuple):
                if cur_attr[0] not in cols_map:
                    print("The attribute being selected is not valid.")
                    raise Exception()
                fun_attrs[cur_attr[0]] = fun_attrs.get(cur_attr[0], [])
                fun_attrs[cur_attr[0]].append(
                    "mean" if cur_attr[1] == "average" else cur_attr[1]
                )
            else:
                if cur_attr not in cols_map:
                    print("The attribute being selected is not valid.")
                    raise Exception()
                non_fun_attrs.append(cur_attr)

        if len(query["GROUP BY"]) == 0:
            if len(non_fun_attrs) > 0 and len(fun_attrs) == 0:
                results = (
                    results.drop_duplicates(non_fun_attrs)[non_fun_attrs]
                    if query["DISTINCT"]
                    else results[non_fun_attrs]
                )
                results = results.rename(
                    columns=dict(
                        [
                            (col, f"{cols_map[col]}.{col}")
                            for col in results.columns.values
                        ]
                    )
                )
                return results
            elif len(non_fun_attrs) == 0 and len(fun_attrs) > 0:
                results["group"] = 0
                results = results.groupby(by=["group"]).agg(fun_attrs).reset_index()
                results.columns = [
                    (
                        f"{'average' if col[1] == 'mean'else col[1]}({cols_map[col[0]]}.{col[0]})"
                        if len(col[1]) > 0
                        else col[0]
                    )
                    for col in results.columns.view()
                ]
                return results.drop(["group"], axis=1)
            else:
                print("The query is invalid.")
                raise Exception()
        else:
            group = query["GROUP BY"]
            if any(attr not in group for attr in non_fun_attrs):
                print("The query is invalid.")
                raise Exception()
            results["group"] = 0
            fun_attrs.update({"group": ["sum"]})
            results = results.groupby(by=list(group)).agg(fun_attrs).reset_index()
            del results[("group", "sum")]
            results.columns = [
                (
                    f"{'average' if col[1] == 'mean'else col[1]}({cols_map[col[0]]}.{col[0]})"
                    if len(col[1]) > 0
                    else col[0]
                )
                for col in results.columns.view()
            ]
            for col in list(group):
                if col not in non_fun_attrs:
                    results = results.drop([col], axis=1)
            results = results.rename(
                columns=dict([(col, f"{cols_map[col]}.{col}") for col in non_fun_attrs])
            )
            return results

    @staticmethod
    def handle_order(results, query):
        attr = query["ORDER BY"]
        if isinstance(attr, list):
            if len(attr) == 0:
                return results
            if len(attr) > 1 or attr not in results.columns.values:
                print("There should be one attribute to order by.")
                raise Exception()
        return results.sort_values(by=[attr], ascending=query["ASC"])

    @staticmethod
    def handle_where(results, query):
        cond = query["WHERE"]
        if len(cond) == 0:
            return results
        if any(attr not in results.columns.values for attr in cond[1]):
            print("There is a condition on an invalid attribute.")
            raise Exception()
        return results[eval(cond[0])]
