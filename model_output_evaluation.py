import json

lgesql_electra_model_output_file = "model_output_postprocess/outputs/lgesql_electra/lgesql_electra_dev_output.json"
bridge_model_output_file = "model_output_postprocess/outputs/bridge/bridge_dev_output.json"

gold_file = "datasets/spider/dev_gold.sql"

output_file = "model_output_postprocess/outputs/lgesql_electra/preds.txt"
preds = []

with open(lgesql_electra_model_output_file, 'r') as f:
    data = json.load(f)
    for ex in data:
        preds.append(ex['inferred_query'])

with open(output_file, 'w') as f:
    for p in preds:
        f.write(p)
        f.write('\n')


