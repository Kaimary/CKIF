import os
import json
import csv
import random
from tkinter import S
import click
import time
import gzip
from tqdm import tqdm
from copy import deepcopy
from collections import defaultdict

from spider_utils.utils import read_single_dataset_schema, fix_query_toks_no_value, disambiguate_items2
from datagen.utils import get_low_confidence_generalized_data, calculate_similarity_score
from datagen.sqlgen.utils.sql_tmp_update import sql_nested_query_tmp_name_convert, use_alias
from datagen.dialectgen.bst_traverse import convert_sql_to_dialect
from spider_utils.evaluation.process_sql import get_schema, get_schema_from_json, tokenize
from spider_utils.evaluation.evaluate import Evaluator, build_foreign_key_map_from_json, rebuild_sql
from configs.config import DIR_PATH, SERIALIZE_DATA_DIR, SEMSIMILARITY_TRIPLE_DATA_GZ_FILE

@click.command()
@click.argument("dataset_name", default="spider")
@click.argument("dataset_train_file", type=click.Path(exists=True, dir_okay=False))
@click.argument("dataset_dev_file", type=click.Path(exists=True, dir_okay=False))
@click.argument("model_train_output_file", type=click.Path(exists=True, dir_okay=False))
@click.argument("model_dev_output_file", type=click.Path(exists=True, dir_okay=False))
@click.argument("tables_file", type=click.Path(exists=True, dir_okay=False))
@click.argument("db_dir", type=click.Path(exists=True, dir_okay=True))
def main(dataset_name, dataset_train_file, dataset_dev_file, model_train_output_file, model_dev_output_file, tables_file, db_dir):

    train_data = generate_triples_for_retrieval_model(
        dataset_name, dataset_train_file, model_train_output_file, tables_file, db_dir
    )
    dev_data = generate_triples_for_retrieval_model(
        dataset_name, dataset_dev_file, model_dev_output_file, tables_file, db_dir, hit=1, mode='dev'
    )
    # Write out into file
    tmp_file = "./tmp.tsv"
    with open(tmp_file, 'wt', ) as tsv_file:
        tsv_writer = csv.writer(tsv_file, delimiter='\t', quoting=csv.QUOTE_NONE, escapechar='\\')
        tsv_writer.writerow(['mode', 'genre', 'dataset', 'year', 'sid', 'score', 'sentence1', 'sentence2'])
        tsv_writer.writerows(train_data)
        tsv_writer.writerows(dev_data)
    print(
        f"Overwrite semantic similarity triple data into file: {SEMSIMILARITY_TRIPLE_DATA_GZ_FILE.format(dataset_name)}.")
    with open(tmp_file, 'rb') as f_in, gzip.open(DIR_PATH + SEMSIMILARITY_TRIPLE_DATA_GZ_FILE.format(dataset_name),
                                                 'wb') as f_out:
        f_out.writelines(f_in)
    os.remove(tmp_file)
    print(f"Overwrite semantic similarity triple data succeed!")

    return

def generate_triples_for_retrieval_model(
        dataset_name, dataset_file, model_output_file, tables_file, db_dir, hit=5, mode='train'
    ):
    """
    Generate `nl-dialect-score`triples for training/validating the retrieval model
    
    The process traverse each of the inferred query,
    1. If the query contains low-confidence marks, execute the generation and serialize the data into file and output
    2. If the query has no low-confidence mark, randomize the marks

    :param dataset_name: the name of NLIDB benchmark
    :param dataset_file: the train/dev/test file
    :param model_output_file: the corresponding inferred results of SODA seq2seq model of the datasest file
    :param tables_file: database schema file
    :param db_dir: the diretory of databases
    :param hit: the threshold for generation
    :param mode: train/dev/test mode
    
    :return: a list of data as the input for retrieval model
    """
    total_count = 0
    output = []
    kmaps = build_foreign_key_map_from_json(tables_file)
    
    schema = {}
    table = {}
    table_dict = {}

    total_triple_2_3_count = 0
    total_triple_3_4_count = 0
    total_triple_4_5_count = 0
    evaluator = Evaluator()
    db_id_cursor = defaultdict(int)
    with open(model_output_file, 'r') as data_file:
        data = json.load(data_file)
        for ex in tqdm(data):
            # if idd < 771: continue
            total_count += 1
            db_id = ex['db_id']
            if db_id not in schema:
                db_file = os.path.join(db_dir, db_id, db_id + ".sqlite")
                if not os.path.isfile(db_file): s = get_schema_from_json(db_id, tables_file)
                else: s = get_schema(db_file)
                _, t, td = read_single_dataset_schema(tables_file, db_id)   
                schema[db_id] = s
                table[db_id] = t
                table_dict[db_id] = td
            # if any(db_id == db for db in ['customers_and_invoices', 'department_store', 'customers_and_addresses', 'formula_1', 'soccer_2']) or \
            #     any(kw in ex['beams'][0]['inferred_code_masked'] for kw in ['join where', 'select from', 'from where', 'from  group']):
            #      continue
            index = ex['index']
            nl = ex['question']
            # Reconstruct gold sql by masking out value tokens
            # Add nl-gold dialect as a positive data
            gold_sql = fix_query_toks_no_value(ex['gold_query_toks_no_value'])
            if 'FROM (' in gold_sql: continue
            try:
                gold_sql = sql_nested_query_tmp_name_convert(gold_sql)
                _, gold_sql_dict, schema_ = disambiguate_items2(tokenize(gold_sql), schema[db_id], table[db_id], allow_aliases=False)
                gold_dialect = convert_sql_to_dialect(gold_sql_dict, table_dict[db_id], schema_)
                output.append([mode, "dialects", "spider", "2021", f"{db_id_cursor[db_id]}", 5.000, nl, gold_dialect])
                db_id_cursor[db_id] += 1
            except: pass
            if not ex['inferred_query_with_marks']: continue
            # Generate sql-dialects
            sqls = []
            dialects = []
            db_data_path = f'{DIR_PATH}{SERIALIZE_DATA_DIR.format(dataset_name)}/gap/{mode}_{index}.txt'
            sqls, dialects = get_low_confidence_generalized_data(
                db_data_path, db_id, ex['inferred_query_with_marks'], ex['inferred_query'],
                dataset_file, tables_file, db_dir, schema[db_id], table[db_id], table_dict[db_id], mode = mode
            )
            # Loop the synthesis sql-dialects to form as negative instances.
            triple_2_3_count = hit
            triple_3_4_count = hit
            triple_4_5_count = hit
            g_sql = rebuild_sql(db_id, db_dir, sql_nested_query_tmp_name_convert(gold_sql), kmaps)
            for irr_sql, irr_dialect in zip(sqls, dialects):
                p_sql = rebuild_sql(db_id, db_dir, sql_nested_query_tmp_name_convert(irr_sql), kmaps)
                if evaluator.eval_exact_match(deepcopy(p_sql), deepcopy(g_sql)) == 1: continue
                score = calculate_similarity_score(g_sql, p_sql)
                if 2 <= score < 3.0 and triple_2_3_count > 0:
                    triple_2_3_count -= 1
                    total_triple_2_3_count += 1
                    output.append([mode, "dialects", "spider", "2021", f"{db_id_cursor[db_id]}", score, nl, irr_dialect])
                    db_id_cursor[db_id] += 1
                elif 3 <= score < 4.0 and triple_3_4_count > 0:
                    triple_3_4_count -= 1
                    total_triple_3_4_count += 1
                    output.append([mode, "dialects", "spider", "2021", f"{db_id_cursor[db_id]}", score, nl, irr_dialect])
                    db_id_cursor[db_id] += 1
                elif 4 <= score < 5.0 and triple_4_5_count > 0:
                    triple_4_5_count -= 1
                    total_triple_4_5_count += 1
                    output.append([mode, "dialects", "spider", "2021", f"{db_id_cursor[db_id]}", score, nl, irr_dialect])
                    db_id_cursor[db_id] += 1

    print(f"Triple(2-3) count:{total_triple_2_3_count}; Triple(3-4) count:{total_triple_3_4_count}; Triple(4-5) count:{total_triple_4_5_count}")
    print(f"mode: {mode} {len(output)} data generated...")
    return output


if __name__ == "__main__":
    main()
#     main("spider", 
#         "datasets/spider/train_spider_6898.json", "datasets/spider/dev.json", 
#         "model_output_postprocess/outputs/gap/gap_train_output.json", 
#         "model_output_postprocess/outputs/gap/gap_dev_output.json",
#         "datasets/spider/tables.json", "datasets/spider/database"
#     )
