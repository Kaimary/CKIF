import os
import click
import json
from tqdm import tqdm

from configs.config import DIR_PATH, SERIALIZE_DATA_DIR
from spider_utils.utils import read_single_dataset_schema
from spider_utils.evaluation.process_sql import get_schema, get_schema_from_json
from datagen.utils import get_low_confidence_generalized_data

@click.command()
@click.argument("dataset_name", default="spider")
@click.argument("model_name", default="gap")
@click.argument("dataset_file", type=click.Path(exists=True, dir_okay=False))
@click.argument("model_output_file", type=click.Path(exists=True, dir_okay=False))
@click.argument("tables_file", type=click.Path(exists=True, dir_okay=False))
@click.argument("db_dir", type=click.Path(exists=True, dir_okay=True))
@click.argument("trial", default=100)
@click.argument("rewrite", default=False)
@click.argument("overwrite", default=False)
@click.argument("mode", default="train")
def main(dataset_name, model_name, dataset_file, model_output_file, tables_file, db_dir, trial, rewrite, overwrite, mode):
    """
    Generalize sql-dialects from low-confidence queries

    :param dataset_name: the name of NLIDB benchmark
    :param model_name: the name of Seq2Seq model
    :param dataset_file: the train/dev/test file
    :param model_output_file: the corresponding inferred results of SODA seq2seq model of the datasest file
    :param tables_file: database schema file
    :param db_dir: the diretory of databases
    :param trial: trial setting in sqlgen
    :param rewrite: if rewriting the dialects (reflect any change that made in dialectgen)
    :param overwrite: if overrite existing serialization files
    :param mode: train/dev/test mode
    
    :return: serialize the data into local files
    """
    total_count = 0
    schema = {}
    table = {}
    table_dict = {}
    serialization_dir = f'{DIR_PATH}{SERIALIZE_DATA_DIR.format(dataset_name)}/{model_name}/{trial}/{mode}'
    with open(model_output_file, 'r') as data_file:
        data = json.load(data_file)
        for ex in tqdm(data):
            total_count += 1
            # if ex['index'] < 817: continue
            db_id = ex['db_id']
            if db_id not in schema:
                db_file = os.path.join(db_dir, db_id, db_id + ".sqlite")
                if not os.path.isfile(db_file): s = get_schema_from_json(db_id, tables_file)
                else: s = get_schema(db_file)
                _, t, td = read_single_dataset_schema(tables_file, db_id)   
                schema[db_id] = s
                table[db_id] = t
                table_dict[db_id] = td
            if not ex['inferred_query_with_marks'] or 'FROM (' in ex['inferred_query_with_marks']: continue
            if not os.path.exists(serialization_dir): os.makedirs(serialization_dir)
            db_data_path = f'{serialization_dir}/{ex["index"]}.txt'
            sqls, _ = get_low_confidence_generalized_data(
                db_data_path, db_id, ex['inferred_query_with_marks'], ex['inferred_query'],
                dataset_file, tables_file, db_dir, schema[db_id], table[db_id], table_dict[db_id], 
                trial=trial, rewrite=rewrite, overwrite=overwrite, mode=mode
            )
            # if sqls: print(f"Generalized size is {len(sqls)}...")

    print(f"Low confidence generalization ({total_count}) has done on {mode} dataset!")
    print(f"Serialization files are saved in the directory: {serialization_dir}")
    return


if __name__ == "__main__":
    main()
#     main("spider", "gap", 
#         "datasets/spider/dev.json", 
#         "model_output_postprocess/outputs/gap/gap_dev_output.json", 
#         "datasets/spider/tables.json", "datasets/spider/database", 
#         trial=1000, rewrite=False, overwrite=False, mode='dev'
#     )