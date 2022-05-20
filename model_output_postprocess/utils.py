from datagen.sqlgenv2.sqlparser import AGG
from spider_utils.evaluation.evaluate import AGG_OPS
from value_mathcing.spider_db_context import is_number

SQL_KEYWORDS = ['SELECT', 'FROM', 'JOIN', 'ON', 'AS', 'WHERE', 'GROUP', 'BY', 'HAVING', \
    'ORDER', 'LIMIT', 'INTERSECT', 'UNION', 'EXCEPT', 'NOT', 'BETWEEN', 'IN', 'LIKE', 'IS', \
        'GROUPBY', 'ORDERBY', 'DISTINCT', 'DESC', 'ASC', 'EXISTS', 'AND', 'OR']
AGG_OPS = ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN']

def sql_string_format(sql):
    """
    Format SQL string 
    1. All columns are represented by  `table.column`
    2. All sql keywords are upper cased and lower-case other tokens
    3. All values are masked with `terminal` using single quotes

    :param sql: the inferred sql string
    :return the sql string with all sql keywords uppercased
    """

    toks = sql.lower().split()
    quote = False
    agg_op = ""
    prev = ""
    for idx, tok in enumerate(toks):
        # " pet dog "
        if '"' in tok and not quote:
            if tok.count('"') == 1: quote = True
            if ')' in tok: toks[idx] = "'terminal')"
            else: toks[idx] = "'terminal'"
        elif '"' in tok and quote:
            quote = False
            if ')' in tok: toks[idx] = ")"
            else: toks[idx] = ""
        elif quote: toks[idx] = ""
        elif is_number(tok) and prev in ['BETWEEN', 'AND', '=', '>=', '<=', '!=', '>', '<']: toks[idx] = "'terminal'"
        elif tok.replace('@', '').replace(')', '').upper() in SQL_KEYWORDS: toks[idx] = tok.upper()
        elif tok.replace('@', '').upper() in AGG_OPS: 
            agg_op = tok.upper()
            toks[idx] = ""
        elif agg_op and tok == ')':
            agg_op += ')'
            toks[idx] = agg_op
            agg_op = ""
        elif agg_op: 
            agg_op += tok
            toks[idx] = ""
        elif '(' in tok:
            parts = tok.split('(')
            # (SELECT
            if not parts[0]: toks[idx] = f'({parts[1].upper()}'
            # @(SELECT
            elif parts[0] == '@': toks[idx] = f'@({parts[1].upper()}'
            # AGG(col)
            else:
                agg = parts[0]
                assert agg.replace('@', '').upper() in AGG_OPS
                other = parts[1]
                # AGG(DISTINCT
                if other.replace('@', '').upper() in SQL_KEYWORDS: toks[idx] = f'{agg.upper()}({other.upper()}'
                else: toks[idx] = f'{agg.upper()}({other}'

        prev = toks[idx]
    return ' '.join(toks)