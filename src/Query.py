# TESTED
import os
import sqlparse
from Print import Print
from Parser import SQLParser
from Handler import Handler


sections = {
    "SELECT": Handler.handle_select,
    "FROM": Handler.handle_from,
    "WHERE": Handler.handle_where,
    "ORDER BY": Handler.handle_order,
}


class Query:
    def __init__(self, query):
        try:
            os.system("clear")
            query = sqlparse.format(query, reindent=True)
            parsed = sqlparse.parse(query)[0]
            if parsed.get_type() not in ["SELECT"]:
                print("There should be a select query.")
                raise Exception()

            query = SQLParser().parse_query(query)
            cols_map, results = sections["FROM"](query)
            results = sections["ORDER BY"](results, query)
            results = sections["WHERE"](results, query)
            results = sections["SELECT"](cols_map, results, query)
            results.reset_index(inplace=True, drop=True)
            Print.print(query, results)

        except:
            print("Error: There is some issue and query not executed.")

    def __del__(self):
        pass
