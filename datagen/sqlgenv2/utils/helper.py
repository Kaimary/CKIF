# -*- coding: utf-8 -*-
# @Time    : 2021/6/4 13:37
# @Author  : 
# @Email   : 
# @File    : sql_tmp_update.py
# @Software: PyCharm
import re
import json
from collections import defaultdict
from datagen.sqlgen.qunit.unit_extract import SQLClauseSeparator, Source

IUE = ['', ' INTERSECT ', ' EXCEPT ', ' UNION ']
IUE_TOKENS = ['INTERSECT', 'EXCEPT', 'UNION']
CLS_TOKENS = ['SELECT', 'FROM', 'WHERE', 'GROUP', 'ORDER']

def sql_nested_query_tmp_name_convert(sql: str, nested_level=0, sub_query_token='S') -> str:
    sql = sql.replace('(', ' ( ')
    sql = sql.replace(')', ' ) ')
    tokens = sql.split()
    select_count = sql.lower().split().count('select')
    level_flag = sub_query_token * nested_level

    # recursive exit
    if select_count == 1:
        # need to fix the last level's tmp name
        res = sql
        if nested_level:
            # log all tmp name
            tmp_name_list = set()
            for i in range(len(tokens)):
                # find tmp name
                if tokens[i].lower() == 'as':
                    tmp_name_list.add(tokens[i + 1])
                # convert every tmp name
            for tmp_name in tmp_name_list:
                res = res.replace(f' {tmp_name}', f' {level_flag}{tmp_name}')
        return res

    # for new sql's token
    new_tokens = list()
    bracket_num = 0
    i = 0
    # iter every token in tokens
    while i < len(tokens):
        # append ordinary token
        new_tokens.append(tokens[i])
        # find a nested query
        if tokens[i] == '(' and tokens[i + 1].lower() == 'select':
            nested_query = ''
            bracket_num += 1
            left_bracket_position = i + 1
            # in one nested query
            while bracket_num:
                i += 1
                if tokens[i] == '(':
                    bracket_num += 1
                elif tokens[i] == ')':
                    bracket_num -= 1
                # to the end of the query
                if bracket_num == 0:
                    # format new nested query and get the tokens
                    nested_query = ' '.join(tokens[left_bracket_position: i])
                    nested_query = sql_nested_query_tmp_name_convert(nested_query, nested_level + 1)
            # new sql's token log
            new_tokens.append(nested_query)
            # append the right bracket
            new_tokens.append(tokens[i])
        # IUE handle
        elif tokens[i].lower() in {'intersect', 'union', 'except'}:
            nested_query = ' '.join(tokens[i + 1:])
            nested_query = sql_nested_query_tmp_name_convert(nested_query, nested_level + 10)
            new_tokens.append(nested_query)
            i += 9999
        i += 1
    # format the new query
    res = ' '.join(new_tokens)
    if nested_level:
        # log all tmp name
        tmp_name_list = set()
        for i in range(len(new_tokens)):
            # find tmp name
            if new_tokens[i].lower() == 'as':
                tmp_name_list.add(new_tokens[i + 1])
            # convert every tmp name
        for tmp_name in tmp_name_list:
            res = res.replace(f' {tmp_name}', f' {level_flag}{tmp_name}')

    return res


def use_alias(sql: str):
    """
    replace original relation name with alias to make sure that the sql unit align
    @param sql: original sql which may use the relation name to specify the col
    @return: new sql which use the alias
    """
    sql = sql.upper()
    # separate the original sql into clause
    sql_clause_separator = SQLClauseSeparator(sql)
    # get the source part for the alias mapping
    source = Source(sql_clause_separator.from_clause)
    # get invert index for replace
    invert_index = dict()
    for alias, relation in source.alias_mapping.items():
        invert_index[relation] = alias
    # replace every relation with alias
    for relation in invert_index:
        sql = sql.replace(f' {relation}.', f' {invert_index[relation]}.')

    sql = sql.replace(' __SingleTable__.', ' ')
    return sql


def split_into_simple_sqls(sql):
    """
    Split SQL with Set Operators(intersect/except/union) into simple SQLs.

    :param sql: complex sql string
    :return: main sql and iue-type sqls if exists

    #TODO support multiple intersects/excepts/unions
    """

    ssql = ""
    intersect_ = ""
    except_ = ""
    union_ = ""
    
    split_indice = []
    tokens = sql.split()
    left_brackets = 0
    for idx, t in enumerate(tokens):
        if '(' in t: left_brackets += 1
        if ')' in t: left_brackets -= 1
        if any(tt in t for tt in IUE_TOKENS) and left_brackets == 0: split_indice.append(idx)
    split_indice.append(len(tokens))

    start = 0
    for i in split_indice:
        if start == 0: ssql = ' '.join(tokens[start: i])
        elif IUE_TOKENS[0] in tokens[start]: intersect_ = ' '.join(tokens[start: i]) 
        elif IUE_TOKENS[1] in tokens[start]: except_ = ' '.join(tokens[start: i]) 
        elif IUE_TOKENS[2] in tokens[start]: union_ = ' '.join(tokens[start: i]) 
        start = i

    return ssql, intersect_, except_, union_

def split_into_clauses(sql):
    """
    Split SQL into clauses(select/from/where/groupby/orderby).

    :param sql: sql string
    :return: sql clause strings
    """

    select_ = ""
    from_   = ""
    where_ = ""
    group_ = ""
    order_ = ""

    split_indice = []
    tokens = sql.split()
    left_brackets = 0
    for idx, t in enumerate(tokens):
        if '(' in t: left_brackets += 1
        if ')' in t: left_brackets -= 1
        if any(tt in t for tt in CLS_TOKENS) and left_brackets == 0:
            split_indice.append(idx)
    split_indice.append(len(tokens))

    assert split_indice[0] == 0
    start = 0
    for i in split_indice[1:]:
        if CLS_TOKENS[0] in tokens[start]: select_ = ' '.join(tokens[start: i])
        elif CLS_TOKENS[1] in tokens[start]: from_ = ' '.join(tokens[start: i]) 
        elif CLS_TOKENS[2] in tokens[start]: where_ = ' '.join(tokens[start: i]) 
        elif CLS_TOKENS[3] in tokens[start]: group_ = ' '.join(tokens[start: i]) 
        elif CLS_TOKENS[4] in tokens[start]: order_ = ' '.join(tokens[start: i]) 
        start = i
    
    return select_, from_, where_,  group_, order_

def split_cls_into_chunks(cls, type):
    """
    Split SQL clause(select/from/where) into the corresponding components(projections/tables/predicates)

    :param cls: sql clause string
    :param type: clause type(select/from/where)
    :return: clause strings

    #TODO support other sql clauses(group/order)
    """
    low_conf_tokens = []
    high_conf_tokens = []
    conj_tokens = []

    # Remove first clause keyword
    tokens = [t.replace(',', '') for t in cls.split()[1:] if t != ',']
    
    add = False
    multi_tokens = []
    if type == 'select':
        # Reconstruct tokens with distinct
        for t in tokens:
            if 'DISTINCT' in t: 
                add = True
                multi_tokens.append(t)
                continue
            elif add:
                multi_tokens.append(t)
                tmp = ' '.join(multi_tokens)
                if '@' in tmp: low_conf_tokens.append(tmp)
                else: high_conf_tokens.append(tmp)
                add = False
            else:
                if '@' in t: low_conf_tokens.append(t)
                else: high_conf_tokens.append(t)
    elif type == 'from':
        for t in tokens:
            if t in ['JOIN', 'ON']: continue
            if '.' in t and not add:
                add = True
                multi_tokens.append(t) 
            elif '.' in t and add:
                multi_tokens.append(t)
                conj_tokens.append(' '.join(multi_tokens))
                multi_tokens.clear()
                add = False
            elif add:
                multi_tokens.append(t)
            else:
                if '@' in t: low_conf_tokens.append(t[1:])
                else: high_conf_tokens.append(t)
    elif type == 'where':
        between = False
        for t in tokens:
            if any(c in t for c in ['.', '=', '>', '<', '!=']) or any(c==t for c in ['NOT', 'IN', 'LIKE', '@NOT', '@IN', '@LIKE']):
                multi_tokens.append(t)
            elif 'BETWEEN' == t:
                between = True
                multi_tokens.append(t)
            elif '(' in t:
                add = True
                multi_tokens.append(t)
            elif ')' in t and add:
                add = False
                multi_tokens.append(t)
                tmp = ' '.join(multi_tokens)
                # TODO predicate with subquery
                high_conf_tokens.append(tmp.replace('@', ''))
                # if '@' in tmp: low_conf_tokens.append(tmp)
                # else: high_conf_tokens.append(tmp)
                multi_tokens.clear()
            elif add:
                multi_tokens.append(t)
            elif "'" in t:
                if not between:
                    multi_tokens.append(t)
                    tmp = ' '.join(multi_tokens)
                    if '@' in tmp: low_conf_tokens.append(tmp)
                    else: high_conf_tokens.append(tmp)
                    multi_tokens.clear()
                else:
                    multi_tokens.append(t)
            elif any(conj in t for conj in ['AND', 'OR', '@AND', '@OR']):
                if between: 
                    between = False
                    multi_tokens.append(t)
                else: conj_tokens.append(t)
    
    return low_conf_tokens, high_conf_tokens, conj_tokens
    
def remove_duplicate_elms(l):
    res = []
    s = set()
    for e in l:
        e = e.replace('@', '')
        if e not in s:
            s.add(e)
            res.append(e)
    res.sort()

    return res

def reorder_from_group(sql):
    
    scls, fcls, wcls,  gcls, ocls = split_into_clauses(sql)

    return ' '.join([scls, fcls, wcls,  gcls, ocls])

# For RAT-SQL+GAP model
# (the model does not generate join conditions)
def add_join_conditions(from_, tables_file, db_id):
    def _find_shortest_path(start, end, graph):
        stack = [[start, []]]
        visited = set()
        while len(stack) > 0:
            ele, history = stack.pop()
            if ele == end:
                return history
            for node in graph[ele]:
                if node[0] not in visited:
                    stack.append((node[0], history + [(node[0], node[1])]))
                    visited.add(node[0])

    dbs_json_blob = json.load(open(tables_file, "r"))
    graph = defaultdict(list)
    table_list = []
    dbtable = {}
    for table in dbs_json_blob:
        if db_id == table['db_id']:
            dbtable = table
            for acol, bcol in table["foreign_keys"]:
                t1 = table["column_names"][acol][0]
                t2 = table["column_names"][bcol][0]
                graph[t1].append((t2, (acol, bcol)))
                graph[t2].append((t1, (bcol, acol)))
            table_list = [table for table in table["table_names_original"]]

    table_alias_dict = {}
    idx = 1

    tables = [t.lower() for t in from_.split() if t not in ['JOIN', 'FROM']]
    prev_table_count = len(tables)
    candidate_tables = []
    for table in tables:
        for i, table1 in enumerate(table_list):
            if table1.lower() == table:
                candidate_tables.append(i)
                break

    ret = ""
    after_table_count = 0
    if len(candidate_tables) > 1:
        start = candidate_tables[0]
        table_alias_dict[start] = idx
        idx += 1
        ret = "FROM {}".format(dbtable["table_names_original"][start].lower())
        after_table_count += 1
        try:
            for end in candidate_tables[1:]:
                if end in table_alias_dict:
                    continue
                path = _find_shortest_path(start, end, graph)
                # print("got path = {}".format(path))
                prev_table = start
                if not path:
                    table_alias_dict[end] = idx
                    idx += 1
                    ret = ""
                    continue
                for node, (acol, bcol) in path:
                    if node in table_alias_dict:
                        prev_table = node
                        continue
                    table_alias_dict[node] = idx
                    idx += 1
                    # print("test every slot:")
                    # print("table:{}, dbtable:{}".format(table, dbtable))
                    # print(dbtable["table_names_original"][node])
                    # print(dbtable["table_names_original"][prev_table])
                    # print(dbtable["column_names_original"][acol][1])
                    # print(dbtable["table_names_original"][node])
                    # print(dbtable["column_names_original"][bcol][1])
                    ret = "{} JOIN {} ON {}.{} = {}.{}".format(ret, dbtable["table_names_original"][node].lower(),
                                                                dbtable["table_names_original"][prev_table].lower(),
                                                                dbtable["column_names_original"][acol][1].lower(),
                                                                dbtable["table_names_original"][node].lower(),
                                                                dbtable["column_names_original"][bcol][1].lower())
                    after_table_count += 1
                    prev_table = node

        except:
            print("\n!!Exception in adding join conditions!!")

        return ret, prev_table_count == after_table_count
    else: return from_, True

def fix_missing_join_condition(sql, db_id, tables_file):
    if 'JOIN' not in sql or ' ON ' in sql: return sql

    new_sql = ""
    simple_sql_, intersect_, except_, union_ = split_into_simple_sqls(sql)

    for idx, s in enumerate([simple_sql_, intersect_, except_, union_]):
        if s:
            if idx > 0: s = ' '.join(s.split()[1:])
            if not s: new_sql += f'@@{IUE[idx].strip()} '
            else:
                scls, fcls, wcls,  gcls, ocls = split_into_clauses(s)
                fcls1, _ = add_join_conditions(fcls.replace('@', ''), tables_file, db_id)
                if not fcls1: return ""
                # retain low-confidence tags
                tokens = fcls.split()
                tokens1 = fcls1.split()
                for i, t in enumerate(tokens1):
                    for tt in tokens:
                        if '@' in tt and tt[1:] == t:
                            tokens1[i] = f'@{t}'
                fcls1 = ' '.join(tokens1)

                if ' JOIN ' in wcls: 
                    f_sp = wcls.index('FROM')
                    wcls_1 = wcls[:f_sp]
                    f_ep = len(wcls) - 1
                    if 'WHERE' in wcls[f_sp:]:
                        f_ep = wcls.rindex('WHERE')
                    elif 'GROUP' in wcls[f_sp:]:
                        f_ep = wcls.rindex('GROUP')
                    elif 'ORDER' in wcls[f_sp:]:
                        f_ep = wcls.rindex('ORDER')
                    wcls_2, _ = add_join_conditions(wcls[wcls.index('FROM'):f_ep].replace('@', ''), tables_file, db_id)
                    wcls_3 = wcls[f_ep:]
                    wcls = ' '.join([wcls_1, wcls_2, wcls_3])
                ss = ' '.join([scls, fcls1, wcls,  gcls, ocls])
                if idx > 0: new_sql += IUE[idx]
                new_sql += ss

    return new_sql
            
def replace_same_name_cols(cls, tables, schema):
    col_col = {}

    tokens = cls.split()
    subquery = False
    for t in tokens:
        if t == '(SELECT': subquery = True
        if ')' in t and '(' not in t: subquery = False
        if '.' not in t or subquery: continue
        
        column = t
        if '(' in t: 
            right = t.split('(')[1]
            column = right.replace('@', '').replace(')', '')
        
        if all(tt not in column for tt in tables):
            col_name = column.split('.')[1]
            new_tab_name = [t for t in tables for c in schema[t.lower()] if col_name in c]
            col_col[column] = f"{new_tab_name[0]}.{col_name}"
    
    for k, v in col_col.items():
        cls = cls.replace(k, v)

    return cls
            
def sql_correct(sql):
    

    # if 'join' in from_ and ' on ' not in from_: from_ = add_join_conditions(from_, self.tables_file, db_id=self.current_db_id)

    return sql