import json
import os
from spider_utils.evaluation.evaluate import build_foreign_key_map_from_json
from datagen.sqlgen.utils.sql_tmp_update import sql_nested_query_tmp_name_convert
from spider_utils.evaluation.process_sql import get_schema, Schema, get_schema_from_json, get_sql
from spider_utils.evaluation.evaluate import Evaluator, build_valid_col_units, rebuild_sql_val, rebuild_sql_col

evaluator = Evaluator()
db_dir = 'datasets/spider/database'
dev_file = "datasets/spider/dev.json"
tables_file = "datasets/spider/tables.json"
output = "GAP_failed_output.txt"
# output = "GAP_pass_output.txt"
model_output="output_failed.json"
# model_output="output_pass.json"
testfile = "output/spider/reranker/test.json"
# testfile = "output/spider/reranker/test_pass.json"

golds = {}
with open(dev_file, 'r') as f:
    for idx, ex in enumerate(json.load(f)):
        golds[idx] = ex['query']

# with open(model_output, 'r') as f:
#     for ex in json.load(f):
#         golds[ex['index']] = ex['gold']

output_idx = []
out = open(output, 'r')
indice = out.readlines()
indice = [int(i.strip()) for i in indice]

kmaps = build_foreign_key_map_from_json(tables_file)


total = 0
bingo = 0
with open(testfile, 'r') as test:
    for idx, ex in enumerate(json.load(test)):
        total += 1
        kmap = kmaps[ex['db_id']]
        p_str = ex['candidate_sqls'][indice[idx]]
        g_str = golds[ex['index']]

        db = os.path.join(db_dir, ex['db_id'], ex['db_id'] + ".sqlite")
        schema = Schema(get_schema(db))
        g_sql = get_sql(schema, g_str)
        p_sql = get_sql(schema, sql_nested_query_tmp_name_convert(p_str))

        g_valid_col_units = build_valid_col_units(g_sql['from']['table_units'], schema)
        g_sql = rebuild_sql_val(g_sql)
        g_sql = rebuild_sql_col(g_valid_col_units, g_sql, kmap)
        p_valid_col_units = build_valid_col_units(p_sql['from']['table_units'], schema)
        p_sql = rebuild_sql_val(p_sql)
        p_sql = rebuild_sql_col(p_valid_col_units, p_sql, kmap)
        exact_score = evaluator.eval_exact_match(p_sql, g_sql)
        if exact_score != 0:
            # print("bingo!!!")
            bingo += 1
        # else:
            # print("FAIL.....")
            # print(f"{ex['index']}")
            # print(f"question: {ex['question']}")
            # print(f"predict:{p_str}")
            # print(f"gold: {g_str}")
            # print("===============================================================================")
        
print(total)
print(bingo)