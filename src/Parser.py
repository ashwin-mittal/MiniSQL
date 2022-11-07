# TESTED
import sqlparse
from sqlparse import parse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword


class SQLParser(object):
    def parse_query(self, statement):
        def parse_tokens(tokens):
            is_identifier = lambda token: token._get_repr_name() == "Identifier"
            is_function = lambda token: token._get_repr_name() == "Function"
            is_comparison = lambda token: token._get_repr_name() == "Comparison"

            ###################################################################

            def strip_tokens(tokens, punctuation=None):
                if punctuation is None:
                    return [
                        token
                        for token in tokens
                        if not token.is_whitespace
                        and token._get_repr_name() != "Comment"
                    ]
                return [
                    token
                    for token in tokens
                    if not token.is_whitespace
                    and token._get_repr_name() != "Comment"
                    and token.ttype != sqlparse.tokens.Token.Punctuation
                ]

            ###################################################################

            def extract_from_part(parsed):
                from_seen = False
                for item in parsed:
                    if from_seen:
                        if item.ttype is Keyword:
                            return
                        else:
                            yield item
                    elif item.ttype is Keyword and item.value.upper() == "FROM":
                        from_seen = True

            def extract_table_identifiers(token_stream):
                for item in token_stream:
                    if isinstance(item, IdentifierList):
                        for identifier in item.get_identifiers():
                            yield identifier.get_name()
                    elif isinstance(item, Identifier):
                        yield item.get_name()
                    elif item.ttype is Keyword:
                        yield item.value

            def parse_from(parsed):
                stream = extract_from_part(parsed)
                return list(extract_table_identifiers(stream))

            ###################################################################

            def col_identifier(token):
                tokens = token.tokens
                tokens = strip_tokens(tokens)
                if len(tokens) == 1:
                    identifier = tokens[0].value
                    return identifier
                if is_identifier(tokens[0]):
                    return col_identifier(tokens[0])
                identifier = tokens[-1].value
                return identifier

            def sql_function(token):
                tokens = token.tokens
                fn, parens = tokens
                col = parens.tokens[1]
                fn = fn.value.lower()
                col = col_identifier(col)
                return col, fn

            def identifier_list(token):
                if is_identifier(token):
                    return col_identifier(token)

                if is_function(token):
                    return [sql_function(token)]

                tokens = token.tokens
                if len(tokens) == 1:
                    if is_function(tokens[0]):
                        return sql_function(tokens[0])
                    return col_identifier(token)

                processed = []
                for token in tokens:
                    if is_identifier(token):
                        processed.append(col_identifier(token))
                    elif is_function(token):
                        col, fn = sql_function(token)
                        processed.append((col, fn))
                    elif (
                        not token.is_whitespace
                        and token.ttype != sqlparse.tokens.Punctuation
                    ):
                        processed.append(col_identifier(token))

                return processed

            def parse_select(tokens):
                identifiers = []
                tokens = strip_tokens(tokens)
                for _, token in enumerate(tokens):
                    if token.ttype is sqlparse.tokens.Wildcard:
                        return
                    elif is_identifier(token):
                        identifiers = [col_identifier(token)]
                    elif token.is_group:
                        identifiers = identifier_list(token)
                return identifiers

            ###################################################################

            def comparison(comps, operators=None):
                identifiers = {}

                def compare_str(comp):
                    comparison_map = {
                        "=": "==",
                        "<>": "!=",
                    }
                    comp = strip_tokens(comp)
                    assert len(comp) == 3
                    col, comp, val = comp
                    comp = comparison_map.get(comp.value, comp.value)
                    if is_function(col):
                        col, fn = sql_function(col)
                        col_str = (col + "_" + fn).replace(".", "_")
                        identifiers[col_str] = col, fn
                    elif col.is_group:
                        col = col_identifier(col)
                        col_str = col.replace(".", "_")
                        identifiers[col_str] = col, None
                    if val.is_group:
                        val = col_identifier(val)
                        identifiers[val.replace(".", "_")] = val, None
                    else:
                        val = val.value
                    val_str = val.replace(".", "_")
                    try:
                        if isinstance(int(val_str), int):
                            return """(results['{col}'] {comp} {val})""".format(
                                col=col_str, comp=comp, val=val_str
                            )
                    except:
                        return """(results['{col}'] {comp} results['{val}'])""".format(
                            col=col_str, comp=comp, val=val_str
                        )

                comp = comps[0]
                eval_str = compare_str(comp)
                if operators is not None:
                    for comp, operator in zip(comps[1:], operators):
                        if operator.upper() == "AND":
                            eval_str += " & " + compare_str(comp)
                        elif operator.upper() == "OR":
                            eval_str += " | " + compare_str(comp)

                return [eval_str, list(identifiers.keys())]

            def parse_where(tokens):
                comps = [token.tokens for token in tokens if is_comparison(token)]
                operators = [
                    token.value for token in tokens if token.value in ("AND", "OR")
                ]
                return comparison(comps, operators)

            ###################################################################

            def parse_group(tokens):
                for token in tokens:
                    if token.is_group:
                        group_by = list(zip(*identifier_list(token)))[0]
                return group_by

            ###################################################################

            def parse_order(tokens):
                for token in tokens:
                    if token.is_group:
                        identifiers = identifier_list(token)
                return identifiers

            ###################################################################

            sections = {
                "SELECT": parse_select,
                "FROM": parse_from,
                "WHERE": parse_where,
                "GROUP BY": parse_group,
                "ORDER BY": parse_order,
            }

            tokens = strip_tokens(tokens)

            _parsed = {
                "SELECT": [],
                "FROM": [],
                "WHERE": [],
                "GROUP BY": (),
                "ORDER BY": [],
                "DISTINCT": False,
                "ASC": True,
            }
            for index, token in enumerate(tokens):
                if index == 0:
                    start = 0
                    curr_sect = token.value.upper()
                    continue
                if token._get_repr_name().upper() == "WHERE":
                    _parsed[curr_sect] = sections[curr_sect](tokens[start:index])
                    curr_sect = "WHERE"
                    _parsed["WHERE"] = sections["WHERE"](token.tokens)
                    continue
                if (
                    token.value.upper() in sections.keys()
                    and token.ttype in sqlparse.tokens.Keyword
                ):
                    if curr_sect != "WHERE":
                        _parsed[curr_sect] = sections[curr_sect](tokens[start:index])
                    start = index
                    curr_sect = token.value.upper()
                if token.value.upper() == "DISTINCT":
                    _parsed["DISTINCT"] = True
                if token.value.upper().find("DESC") > 0:
                    _parsed["ASC"] = False

            if curr_sect != "WHERE":
                _parsed[curr_sect] = sections[curr_sect](tokens[start:])

            return _parsed

        tokens = parse(statement)[0].tokens
        return parse_tokens(tokens)
