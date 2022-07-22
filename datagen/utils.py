import os
import random
from copy import deepcopy

from datagen.sqlgenv2.sqlgen import GeneratorV2
from spider_utils.utils import disambiguate_items2
from spider_utils.evaluation.evaluate import Evaluator
from spider_utils.evaluation.process_sql import tokenize
from datagen.sqlgen.utils.sql_tmp_update import sql_nested_query_tmp_name_convert, use_alias
from datagen.dialectgen.bst_traverse import convert_sql_to_dialect
from spider_utils.evaluation.evaluate import build_foreign_key_map_from_json, rebuild_sql

# Compare the SQL structures and calculate a similarity score between the two.
def calculate_similarity_score(g_sql, p_sql):
    evaluator = Evaluator()
    total_score = 5.0
    # We first check the sources of the two sqls, we assume it dominants the similarity score.
    if len(g_sql['from']['table_units']) > 0:
        label_tables = sorted(g_sql['from']['table_units'])
        pred_tables = sorted(p_sql['from']['table_units'])
        if label_tables != pred_tables:
            total_score -= 1.0
        elif len(g_sql['from']['conds']) > 0:
            label_joins = sorted(g_sql['from']['conds'], key=lambda x: str(x))
            pred_joins = sorted(p_sql['from']['conds'], key=lambda x: str(x))
            if label_joins != pred_joins:
                total_score -= 0.5
    partial_scores = evaluator.eval_partial_match(deepcopy(p_sql), deepcopy(g_sql))
    # Next we use 7 of 10 categories from partial scores to do the comparison: 
    # 1)select 2)where 3)group 4)order 5)and/or 6)IUE 7)keywords
    for category, score in partial_scores.items():
        if score['f1'] != 1:
            if category == "keywords":
                total_score -= 0.5
            elif category == "select":
                total_score -= 1.0
            elif category == "where":
                total_score -= 0.5
            elif category == "group":
                total_score -= 0.5
            elif category == "order":
                total_score -= 0.5
            elif category == "and/or":
                total_score -= 0.2
            elif category == "IUEN":
                total_score -= 0.8

    return total_score


def get_low_confidence_generalized_data(
    serialization_file, db_id, inferred_query_with_marks, inferred_query,
    dataset_file, tables_file, db_dir, 
    schema, table, table_dict,
    trial=100, rewrite=False, overwrite=False, mode='train'
):
    """
    Get the generalized sql-dialects from a low-confidence query
    The generalization process will serialize the generalized data into local files.
        1. If the query contains low-confidence marks, execute the generation and serialize the data into file and output
        2. If the query has no low-confidence mark or fails to generate, serialize into an empty file and skip


    :param dataset_name: the name of NLIDB benchmark
    :param dataset_file: the train/dev/test file
    :param model_output_file: the corresponding inferred results of SODA seq2seq model of the datasest file
    :param tables_file: database schema file
    :param db_dir: the diretory of databases
    :param overwrite: if overrite existing serialization files
    :param mode: train/dev/test mode
    
    :return: serialize the data into local files
    """
    global kmaps
    kmaps = build_foreign_key_map_from_json(tables_file)
    evaluator = Evaluator()

    sqls = []
    dialects = []
    if not os.path.exists(serialization_file) or overwrite:
        # Create an empty serialization file first
        datafile = open(serialization_file, 'w')
        gen2 = GeneratorV2(dataset_file, tables_file, db_dir, trial=trial)
        gen2.load_database(db_id)
        if not inferred_query_with_marks: return sqls, dialects
        # For training purpose, we randomize to add low-confidence marks to augment the training size
        if mode =="train" and '@' not in inferred_query_with_marks:
            tokens = inferred_query_with_marks.split()
            tokens1 = [1 for t in tokens if t not in ["DESC", "ASC", "ON", "JOIN", "HAVING", "'terminal'", "BY", "DISTINCT"]]
            max = 6 if len(tokens1) > 6 else len(tokens1)
            num = random.randint(2, max)
            while num:
                i = random.randint(0, len(tokens) - 1)
                if '@' in tokens[i] or tokens[i] in ["DESC", "ASC", "ON", "JOIN", "HAVING", "'terminal'", "BY", "DISTINCT"]: continue
                tokens[i] = f'@{tokens[i]}'
                num -= 1
            inferred_query_with_marks = ' '.join(tokens)
        # sqls_ = gen2.generate(inferred_query_with_marks, inferred_query)
        try: sqls_ = gen2.generate(inferred_query_with_marks, inferred_query)
        except:
            print(f"ERR in SQLGenV2 - {db_id}: {inferred_query_with_marks}")
            os.remove(serialization_file)
            return sqls, dialects
        for sql in sqls_:
            try:
                sql = sql_nested_query_tmp_name_convert(sql)
                sql = use_alias(sql)
                _, sql_dict, schema_ = disambiguate_items2(tokenize(sql), schema, table, allow_aliases=False)
                dialect = convert_sql_to_dialect(sql_dict, table_dict, schema_)
                sqls.append(sql)
                dialects.append(dialect)
            except: pass
        # Invalid sql
        if not sqls: return sqls, dialects
        # If only one sql left, check if it is the same with original one, 
        # since the generation may revise the syntax/semantics error in the orignal sql
        if len(sqls) == 1:
            p_sql  = rebuild_sql(db_id, db_dir, sql_nested_query_tmp_name_convert(inferred_query), kmaps, tables_file)
            p_sql1 = rebuild_sql(db_id, db_dir, sql_nested_query_tmp_name_convert(sqls[0]), kmaps, tables_file)
            if evaluator.eval_exact_match(deepcopy(p_sql), deepcopy(p_sql1)) == 1: return [], []
        # Serialize the genration results
        # Each line includes a sql and the corresponding dialect
        for sql, dialect in zip(sqls, dialects):
            # write line to output file
            line = f'{sql}\t{dialect}\n'
            datafile.write(line)
        datafile.close()
    # Read from the existing serialization file if exists
    else:
        # Skip empty serizalization 
        if os.stat(serialization_file).st_size == 0: return sqls, dialects
        all_lines = []
        datafile = open(serialization_file, 'r')
        for line in datafile.readlines():
            sql, dialect = line.split('\t')
            if rewrite:
                sql = sql_nested_query_tmp_name_convert(sql)
                _, sql_dict, schema_ = disambiguate_items2(tokenize(sql), schema, table, allow_aliases=False)
                dialect = convert_sql_to_dialect(sql_dict, table_dict, schema_)
                line = sql + '\t' + dialect + '\n'
                all_lines.append(line)
            sqls.append(sql.strip())
            dialects.append(dialect.strip())
        if rewrite:
            datafile = open(serialization_file, 'w')
            datafile.writelines(all_lines)
            datafile.close()
    
    return sqls, dialects