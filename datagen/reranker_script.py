import random
import numpy as np
import json
import os
import click
from tqdm import tqdm
from copy import deepcopy
import faiss
from sentence_transformers import SentenceTransformer
from spider_utils.utils import read_single_dataset_schema, disambiguate_items2
from datagen.sqlgenv2.utils.helper import sql_nested_query_tmp_name_convert
from datagen.dialectgen.bst_traverse import convert_sql_to_dialect
from datagen.utils import get_low_confidence_generalized_data
from spider_utils.evaluation.process_sql import get_schema, get_schema_from_json, tokenize
from spider_utils.evaluation.evaluate import build_foreign_key_map_from_json, rebuild_sql
from spider_utils.evaluation.evaluate import Evaluator
from configs.config import DIR_PATH, SERIALIZE_DATA_DIR, RETRIEVAL_MODEL_DIR, \
    RETRIEVAL_MODEL_EMBEDDING_DIMENSION
from spider_utils.recall_checker_utils import RecallChecker


@click.command()
@click.argument("dataset_name", default="spider")
@click.argument("model_name", default="gap")
@click.argument("retrieval_model_name", default="nli-distilroberta-base-v2")
@click.argument("dataset_file", type=click.Path(exists=True, dir_okay=False))
@click.argument("model_output_file", type=click.Path(exists=True, dir_okay=False))
@click.argument("tables_file", type=click.Path(exists=True, dir_okay=False))
@click.argument("db_dir", type=click.Path(exists=True, dir_okay=True))
@click.argument("candidate_num", default=300)
@click.argument("trial", default=100)
@click.argument("rewrite", default=False)
@click.argument("overwrite", default=False)
@click.argument("mode", default="train")
@click.argument("debug", default=False)
@click.argument("output_file", type=click.Path(exists=False, dir_okay=False))
def main(
    dataset_name, model_name, retrieval_model_name, 
    dataset_file, model_output_file, tables_file, db_dir, 
    candidate_num, trial, rewrite, overwrite, mode, debug, output_file
    ):
    """
    Generalize inferred queries with low-confidence marks and the corresponding dialects
    as the input for re-ranking model for further training/testing

    :param dataset_name: the name of NLIDB benchmark
    :param model_name: the seq2seq model name
    :param retrieval_model_name: the name of the trained retrieval model
    :param dataset_file: the train/dev/test file
    :param model_output_file: the corresponding inferred results of SODA seq2seq model of the datasest file
    :param tables_file: database schema file
    :param db_dir: the diretory of databases
    :param candidate_num: the filtered candiate number of the retrieval model
    :param trial: trial setting in sqlgen
    :param rewrite: if rewriting the dialects (reflect any change that made in dialectgen)
    :param overwrite: if overrite existing serialization files
    :param mode: train/dev/test mode
    :param debug: debug mode
    :param output_file: the output file

    :return: a list of data as the input for reranking model
    """
    # Initialization
    schema = {}
    table = {}
    table_dict = {}
    serialization_dir = f'{DIR_PATH}{SERIALIZE_DATA_DIR.format(dataset_name)}/{model_name}/{trial}/{mode}'
    if not os.path.exists(serialization_dir): os.makedirs(serialization_dir)
    kmaps = build_foreign_key_map_from_json(tables_file)
    evaluator = Evaluator()
    # Load the trained retrieval model
    embedder = SentenceTransformer(
        DIR_PATH + RETRIEVAL_MODEL_DIR.format(dataset_name) + '/' + retrieval_model_name)
    checker = RecallChecker(dataset_file, tables_file, db_dir)
    if debug: 
        # Statistics (debug purpose)
        model_corr_num = 0
        model_incorr_num = 0
        low_conf_in_corr_num = 0
        low_conf_in_incorr_num = 0
        # For correct inferred queries, the generation always hits as the original inferred query keeps. 
        # Therefore, the following two counts are only for incorrect inferred queries.
        hit_gen_num = 0 
        corr_go_gen_num = 0
        miss_gen_num = 0
        failed_generation = 0

    output = []
    with open(model_output_file, "r") as data_file:
        data = json.load(data_file)
        total_count = 0
        for ex in tqdm(data):
            total_count += 1
            
            db_id = ex['db_id']
            if db_id not in schema:
                db_file = os.path.join(db_dir, db_id, db_id + ".sqlite")
                s = get_schema_from_json(db_id, tables_file) if not os.path.isfile(db_file) else get_schema(db_file)
                _, t, td = read_single_dataset_schema(tables_file, db_id)   
                schema[db_id] = s
                table[db_id] = t
                table_dict[db_id] = td

            question = ex['question']
            index = ex['index']
            if debug:
                if ex['exact']: model_corr_num += 1
                else: model_incorr_num += 1
                if '@' in ex['inferred_query_with_marks']:
                    if ex['exact']: low_conf_in_corr_num += 1
                    else: low_conf_in_incorr_num += 1
            
            serialization_file = f'{serialization_dir}/{ex["index"]}.txt'
            # Generate possible sqls (and dialects)
            sqls, dialects = get_low_confidence_generalized_data(
                serialization_file, db_id, ex['inferred_query_with_marks'], ex['inferred_query'],
                dataset_file, tables_file, db_dir, schema[db_id], table[db_id], table_dict[db_id],
                trial=trial, rewrite=rewrite, overwrite=overwrite, mode=mode
            )
            if not sqls: continue
            
            hit = False
            # Check if the generation hits for incorrect inferred results
            if debug and not ex['exact']:
                try:
                    g_sql = rebuild_sql(db_id, db_dir, sql_nested_query_tmp_name_convert(ex['gold_query']), kmaps, tables_file)
                    for sql in sqls:
                        p_sql = rebuild_sql(db_id, db_dir, sql_nested_query_tmp_name_convert(sql), kmaps, tables_file)
                        if evaluator.eval_exact_match(deepcopy(p_sql), deepcopy(g_sql)) == 1:
                            hit_gen_num += 1
                            hit = True
                            break
                except: pass
                if not hit:
                    if len(sqls) == 1: failed_generation += 1
                        # print("<Low-confidence existence but failed to generate...>")
                        # print(f"{index} gold sql: {ex['gold_query']}")
                        # print(f"inference sql: {ex['inferred_query_with_marks']}")
                        # print("===============================================================================================================================")
                    else: 
                        miss_gen_num += 1
                        print("<Generate but Miss>")
                        print(f"{index} gold sql: {ex['gold_query']}")
                        print(f"inference sql: {ex['inferred_query_with_marks']}")
                        print("===============================================================================================================================")
            elif debug and ex['exact']: corr_go_gen_num += 1

            # Make sure the generated number fixes to 100
            # while len(sqls) < candidate_num:
            #     # add the first sql repeately
            #     sqls.append(sqls[0])
            #     dialects.append(dialects[0])

            num = len(sqls) if len(sqls) < candidate_num else candidate_num
            # Get the top-k sql-dialect pairs
            question_embedding = embedder.encode(question)
            dialect_embeddings = embedder.encode(dialects)
            fidx = faiss.IndexFlatL2(int(RETRIEVAL_MODEL_EMBEDDING_DIMENSION))
            fidx.add(np.stack(dialect_embeddings, axis=0))
            _, indices = fidx.search(np.asarray(question_embedding).reshape(1, int(RETRIEVAL_MODEL_EMBEDDING_DIMENSION)), num)
            candidate_dialects = [dialects[indices[0, idx]] for idx in range(0, num)]
            candidate_sqls = [sqls[indices[0, idx]] for idx in range(0, num)]
            # If the geneartion fits for incorrect inferred results or those correct inferred results
            # Check the precision for the retrieval model
            gold_sql_indices = []
            if mode == 'train' or mode == 'dev' or (mode == 'test' and debug and (hit or ex['exact'])): 
                try:
                    gold_sql_indices = \
                        checker.check_add_candidategen_miss_sql(db_id, candidate_sqls, ex['gold_query'], ex['exact'])
                except: continue

            # For training/validation purpose, add gold sql back if not exists in the candiates
            if mode == "train" or mode == "dev":
                if not gold_sql_indices:
                    try:
                        gold_sql = sql_nested_query_tmp_name_convert(ex['gold_query'])
                        _, sql_dict, schema_ = disambiguate_items2(tokenize(gold_sql), schema[db_id], table[db_id], allow_aliases=False)
                        gold_sql_dialect = convert_sql_to_dialect(sql_dict, table_dict[db_id], schema_)
                        candidate_sqls.pop()
                        candidate_sqls.append(ex['gold_query'])
                        candidate_dialects.pop()
                        candidate_dialects.append(gold_sql_dialect)
                        gold_sql_indices.append(num-1)
                    except: continue
            
            while num < candidate_num:
                candidate_sqls.append(sqls[0])
                candidate_dialects.append(dialects[0])
                if 0 in gold_sql_indices: gold_sql_indices.append(num)
                num += 1

            # Construct the listwise instance
            # if mode == "train":
            #     # For training purpose, we split the data into the list with 10 size
            #     for j in range(0, (int)(candidate_num / 80)):
            #         start = j * 80
            #         end = (j + 1) * 80
            #         candidates = candidate_dialects[start: end]
            #         labels = [1 if i in gold_sql_indices else 0 for i in range(start, end)]
            #         if 1 not in labels:
            #             candidates.pop()
            #             labels.pop()
            #             candidates.append(candidate_dialects[gold_sql_indices[0]])
            #             labels.append(1)
            #         # Shuffle the list
            #         c = list(zip(candidates, labels))
            #         random.shuffle(c)
            #         candidates, labels = zip(*c)
            #         ins = {
            #             "index": index,
            #             "db_id": db_id,
            #             "question": question,
            #             "candidates": candidates,
            #             "labels": labels
            #         }
            if mode == 'train' or mode == 'dev':
                labels = [1 if i in gold_sql_indices else 0 for i in range(candidate_num)] 
                # Shuffle the list
                c = list(zip(candidate_dialects, labels))
                random.shuffle(c)
                candidate_dialects, labels = zip(*c)
                ins = {
                    "index": index,
                    "db_id": db_id,
                    "question": question,
                    "candidates": candidate_dialects
                }
                ins["labels"] = labels
            else: 
                labels = [1 if i in gold_sql_indices else 0 for i in range(candidate_num)] 
                ins = {
                    "index": index,
                    "db_id": db_id,
                    "question": question,
                    "candidates": candidate_dialects
                }
                ins["candidate_sqls"] = candidate_sqls
                ins["labels"] = labels
            
            output.append(ins)

    print(f"total data: {total_count}")
    print(f"output length: {len(output)}")
    if debug: 
        assert model_corr_num + model_incorr_num == total_count
        print(f"model correct inference count: {model_corr_num}; model incorrect inference count: {model_incorr_num}")
        print(f"low confidence in correct count: {low_conf_in_corr_num}; low confidence in incorrect count: {low_conf_in_incorr_num}")
        print(f"hit generation count (for incorrect inference): {hit_gen_num} miss generation count (for incorrect inference): {miss_gen_num}")
        print(f"correct but go the generation count(for correct inferrence): {corr_go_gen_num}")
        print(f"failed_generation:{failed_generation}")
        checker.print_candidategen_total_result(hit_gen_num+corr_go_gen_num, candidate_num)
        checker.export_candidategen_miss_sqls(dataset_name, model_name)
        if mode == "test": assert hit_gen_num + failed_generation + miss_gen_num + corr_go_gen_num == len(output)

    with open(output_file.format(dataset_name), 'w') as outfile:
        json.dump(output, outfile, indent=4)
        
    return

if __name__ == "__main__":
    main()
#     dataset_file = 'datasets/spider/dev.json' #'datasets/spider/train_spider_6898.json' 
#     model_output_file = 'model_output_postprocess/outputs/gap/gap_dev_output.json'  #'model_output_postprocess/outputs/gap/gap_train_output.json' 
#     output_file =  '/output/{0}/reranker/reranker_dev.json' #'/output/{0}/reranker/reranker_train.json'
#     main(
#         'spider', 'gap', 'sentence_embedder_nli-distilroberta-base-v2', 
#         dataset_file, model_output_file, 'datasets/spider/tables.json', 'datasets/spider/database', 
#         trial=1000, candidate_num=100, rewrite=False, overwrite=False, mode='dev', debug=False, output_file=output_file
#     )
    