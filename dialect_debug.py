import os
from spider_utils.utils import read_single_dataset_schema, disambiguate_items2
from datagen.sqlgenv2.utils.helper import sql_nested_query_tmp_name_convert, use_alias
from datagen.dialectgen.bst_traverse import convert_sql_to_dialect
from spider_utils.evaluation.process_sql import get_schema, get_schema_from_json, tokenize

db_id = "car_1"
db_dir = "datasets/spider/database"
tables_file = "datasets/spider/tables.json"
db_file = os.path.join(db_dir, db_id, db_id + ".sqlite")
schema = get_schema_from_json(db_id, tables_file) if not os.path.isfile(db_file) else get_schema(db_file)
_, table, table_dict = read_single_dataset_schema(tables_file, db_id)   

sql = "SELECT car_names.model FROM car_names JOIN cars_data ON car_names.makeid = cars_data.id   ORDER BY cars_data.mpg DESC LIMIT 1"
sql = sql_nested_query_tmp_name_convert(sql)
sql = use_alias(sql)
_, sql_dict, schema_ = disambiguate_items2(tokenize(sql), schema, table, allow_aliases=False)
dialect = convert_sql_to_dialect(sql_dict, table_dict, schema_)
print(dialect)