import click
import json
import collections

@click.command()
@click.argument("reranker_input_file", type=click.Path(exists=True, dir_okay=False))
@click.argument("value_filtered_output_file", type=click.Path(exists=True, dir_okay=False))
@click.argument("model_output_file", type=click.Path(exists=True, dir_okay=False))
@click.argument("output_file", type=click.Path(exists=False, dir_okay=False))
def main(reranker_input_file, value_filtered_output_file, model_output_file, output_file):
    """
    Get the final inference sqls of GAR

    :param reranker_input_file: the input inference file for reranker model  
    :param value_filtered_output_file: the inference output sql file for reranker model 
    :param model_output_file: the corresponding inferred results of SODA seq2seq model of the datasest file
    :param output_file: the output file name

    :return: the full output sqls of the prediced data
    """

    index_sql_dict = {}
    # First add predicted sqls from reranker model into the dict
    reranker_input = open(reranker_input_file, 'r')
    reranker_sql_output = open(value_filtered_output_file, 'r')
    for ex, line in zip(json.load(reranker_input), reranker_sql_output.readlines()):
        if ex['index'] in index_sql_dict: assert()
        index_sql_dict[ex['index']] = line.strip()

    model_output = open(model_output_file, 'r')
    model_data = json.load(model_output)
    total_count = len(model_data)

    add = 0
    for idx in range(total_count):
        if idx not in index_sql_dict:
            add += 1
            index_sql_dict[idx] = model_data[idx]['inferred_query']

    output = open(output_file, 'w')
    od = collections.OrderedDict(sorted(index_sql_dict.items()))
    for _, v in od.items():
        output.write(v)
        output.write('\n')
    
    print(f"The full output SQLs of GAR is saved in: {output_file}!")
    return

if __name__ == "__main__":
    main()
#     main("output/spider/reranker/test.json", 
#         "output/spider/reranker/pred_sql_value_filtered.json", 
#         "model_output_postprocess/outputs/gap/gap_dev_output.json",
#         "output/spider/reranker/pred_sql_value_filtered_full.json"
#     )