import json
import click
from tqdm import tqdm
from collections import defaultdict

from configs.config import DIR_PATH, POSTPROCESS_OUTPUT_FILE
from model_output_postprocess.utils import sql_string_format
from datagen.sqlgenv2.utils.helper import fix_missing_join_condition

# @click.command()
# @click.argument("model_name", default="gap")
# @click.argument("output_file", type=click.Path(exists=True, dir_okay=False))
# @click.argument("test_file", type=click.Path(exists=True, dir_okay=False))
# @click.argument("tables_file", type=click.Path(exists=True, dir_okay=False))
# @click.argument("validation", type=click.BOOL, default=True)
def main(model_name, model_output_file, test_file, tables_file, validation):
    assert model_name in ['gap', 'ratsql', 'bridge', 'natsql', 'gnn']

    inferred = open(model_output_file)
    test_instances = json.load(open(test_file))

    output_file = f'{DIR_PATH}{POSTPROCESS_OUTPUT_FILE.format(model_name)}'
    output = open(output_file, "w")

    # Format inferred results
    instances = []
    if model_name in ['gap', 'ratsql']:
        # Get the evaluation result for each predicted query
        if validation:
            model_eval_output_file = model_output_file.replace('infer', 'eval')
            eval = open(model_eval_output_file)
            exacts = [ex['exact'] for ex in json.load(eval)['per_item']]
        # idx = 0
        for line in tqdm(list(inferred)):
            data = defaultdict()
            infer_results = json.loads(line)
            index = infer_results['index']
            data['index'] = index
            data['db_id'] = test_instances[index]['db_id']
            data['question'] = test_instances[index]['question']
            data['inferred_query'] = ""
            data['inferred_query_with_marks'] = ""
            if infer_results['beams']:
                inferred_query = sql_string_format(infer_results['beams'][0]['inferred_code'])
                inferred_query_with_marks = sql_string_format(infer_results['beams'][0]['inferred_code_masked'])
                inferred_query = fix_missing_join_condition(inferred_query, data['db_id'], tables_file)
                inferred_query_with_marks = fix_missing_join_condition(inferred_query_with_marks, data['db_id'], tables_file)
                data['inferred_query']  = inferred_query
                data['inferred_query_with_marks'] = inferred_query_with_marks
            if validation:
                data['exact'] = exacts[index]
                data['gold_query'] = test_instances[index]['query']
                data['gold_query_toks_no_value'] = test_instances[index]['query_toks_no_value']

            instances.append(data)
    elif model_name == 'bridge':
        json_obj = json.load(inferred)
        for idx, ex in enumerate(tqdm(json_obj)):
            data = ex
            data['inferred_query'] = sql_string_format(ex['inferred_query'])
            data['inferred_query_with_marks'] = sql_string_format(ex['inferred_query_with_marks'])
            instances.append(data)
            
    json.dump(instances, output, indent=4)


if __name__ == "__main__":
    main('ratsql', 
    'model_output_postprocess/outputs/ratsql/bert_run_true_1-step34100.infer', 
    'datasets/spider/dev.json', 'datasets/spider/tables.json', 
    validation=True)