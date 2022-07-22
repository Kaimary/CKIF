import os
import json
import random

from datagen.sqlgenv2.sqlparser import LowConfidenceSQLParser
from spider_utils.evaluation.evaluate import rebuild_sql, build_foreign_key_map_from_json
from spider_utils.evaluation.process_sql import get_schema, get_schema_from_json
from .utils.helper import fix_missing_join_condition, split_into_simple_sqls, \
    sql_nested_query_tmp_name_convert, reorder_from_group, add_join_conditions, fix_missing_join_condition, sql_correct

IUE = ['', 'INTERSECT', 'EXCEPT', 'UNION']

class GeneratorV2(object):
    def __init__(self, data_file, tables_file, db_dir, trial=100):
        """
        :param data_file: SODA model output file with low-confidence tags
        :param tables_file: The database schemata file.
        :param db_dir: The directory of databases for the dataset.
        """
        self.parser = LowConfidenceSQLParser(tables_file, trial)

        self._trial = trial
        self.schemas = {}
        self.data_file = data_file
        self.tables_file = tables_file
        self.syntax_constraint = {}
        self.db_dir = db_dir
        self.kmaps = build_foreign_key_map_from_json(tables_file)

    def load_database(self, db_name):
        """
        Load the schema of current database.

        :param db_name: database ID
        """
        self.current_db_id = db_name

        db_file = os.path.join(self.db_dir, db_name, db_name + ".sqlite")
        if not os.path.isfile(db_file):
            schema = get_schema_from_json(db_name, self.tables_file)
        else:
            schema = get_schema(db_file)
        self.schemas[db_name] = schema
        # _, self.table, self.table_dict = read_single_dataset_schema(self.tables_file, self.db_name)

    def generate(self, masked_sql, original_sql):
        """
        Generate sqls based on the low-confidence masked SQL as template.
        
        :param masked_sql: sql string with low-confidence tags
        :param original_sql: predicted sql string from SODA model
        :return: a list of generated sqls
        """
        masked_sql = masked_sql
        original_sql = original_sql
        # TODO support from subquery
        if 'FROM (SELECT' in original_sql: return [masked_sql.replace('@', '')]

        masked_simple_sql, masked_intersect, masked_except, masked_union = split_into_simple_sqls(masked_sql)
        simple_sql_, intersect_, except_, union_ = split_into_simple_sqls(original_sql)

        simple_sqls = self.generate_simple_sqls(masked_simple_sql, simple_sql_)
        intersect_sqls = []
        except_sqls = []
        union_sqls = []
        if masked_intersect: intersect_sqls = self.generate_simple_sqls(masked_intersect, intersect_)
        if masked_except: except_sqls = self.generate_simple_sqls(masked_except, except_)
        if masked_union: union_sqls = self.generate_simple_sqls(masked_union, union_)

        sqls = self.generate_compound_sqls(simple_sqls, intersect_sqls, except_sqls, union_sqls)

        return sqls

    def generate_simple_sqls(self, mssql, ssql):
        """
        Generate sqls based on the low-confidence masked SQL as template.
        <The function does the real work>

        :param mssql: simple sql string with low-confidence tags
        :param ssql: the corresponding sql string without low-confidence tags
        :return: a list of generated simple sqls
        """
        # TODO parse and generate iue sqls
        if any(t == mssql for t in ['@@INTERSECT', '@@EXCEPT', '@@UNION']): return []
        elif any(st in mssql for st in ['@INTERSECT', '@EXCEPT', '@UNION']): return ["", ssql[ssql.index('SELECT'):].replace('@', '')]

        # Remove set operator keywords if exists
        mssql = mssql[mssql.index('@SELECT'):] if '@SELECT' in mssql else mssql[mssql.index('SELECT'):]
        ssql = ssql[ssql.index('SELECT'):]
        
        sql_map_set = set()
        schema = self.schemas[self.current_db_id]
        # Initialize parser with the current sql
        self.parser.load(mssql, db_id=self.current_db_id, schema=schema)
        # Global check
        res = self.parser.parse_all()
        # Parse from clause to get associated tables
        tables_list, froms, not_used_tables_list = \
            self.parser.parse_from()
        assert len(tables_list) == len(froms) == len(not_used_tables_list)
        for tables, froms_, not_used_table_list in zip(tables_list, froms, not_used_tables_list):
            assert len(froms_) == len(not_used_table_list)
            for from_, not_used_tables in zip(froms_, not_used_table_list):
                wheres = []
                orders = []
                select_groups = self.parser.parse_select_group(tables, not_used_tables, schema)
                if self.parser.where: wheres = self.parser.parse_where(tables, not_used_tables, schema)
                if self.parser.order: orders = self.parser.parse_order(tables, not_used_tables, schema)
                sqls, sql_map_set_ = self.random_compose(self._trial, True, select_groups, [from_], wheres, orders)
                res.extend(sqls)
                sql_map_set.update(sql_map_set_)

        # ssql = fix_missing_join_condition(ssql, self.current_db_id, self.tables_file)
        # if not ssql: return res
        if self.parser.is_valid_sql:
            p_sql = rebuild_sql(
                    self.current_db_id, self.db_dir, sql_nested_query_tmp_name_convert(ssql), self.kmaps,
                    self.tables_file)
            sql_map = json.dumps(p_sql)
            
            if sql_map not in sql_map_set:
                res.append(ssql)

        return res

    def generate_compound_sqls(self, ssqls, intersects, excepts, unions):
        if not intersects and not excepts and not unions: return ssqls

        compound_sqls, _ = self.random_compose(100, False, ssqls, intersects, excepts, unions)
        
        return compound_sqls

    def random_compose(self, trial, reorder, *argv):
        res = []
        sql_map_set = set()
        hit = 0

        while hit < trial and len(res) < trial * 10:
            sql = []
            for idx, clauses in enumerate(argv):
                if clauses: 
                    cls = random.choice(clauses)
                    if not reorder and cls: 
                        sql.append(IUE[idx])
                    sql.append(cls)
            
            # print(sql)
            s_sql = reorder_from_group(' '.join(sql)) if reorder else ' '.join(sql)
            s_sql = sql_correct(s_sql)
            # print(s_sql)
            p_sql = rebuild_sql(
                self.current_db_id, self.db_dir, sql_nested_query_tmp_name_convert(s_sql), self.kmaps,
                self.tables_file)
            sql_map = json.dumps(p_sql)
            if sql_map not in sql_map_set:
                sql_map_set.add(sql_map)
                res.append(s_sql)
                hit = 0
            else:
                hit += 1

        return res, sql_map_set