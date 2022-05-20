# Generate-and-Rank strategy by detecting low-confidence steps in Seq2Seq translation

# Postprocessing on model raw output
Before making inference with GAR, we need to format the outputs of any Seq2Seq models first. Please check the README in the `model_output_postprocess` folder for the details.

the postprocessing of model `GAP`, `RAT-SQL` and `BRIDGE` has been added in the `model_output_postprocess.py` file in the root directory;

## Training
Using the following command to start training ranking models,
```
bash train_pipeline.sh <benchmark name> <seq2seq model name> <train json file> <dev json file> <seq2seq model train output file> <seq2seq model dev output file> <table schema json file> <sqlite database directory>
```

## Inference
Using the following command to do the inference,
```
bash test_pipeline.sh <benchmark name> <seq2seq model name> <seq2seq model output file> <dev/test json file> <gold sql txt file> <table schema json file> <sqlite database directory>
```

### Output
All the outputs of the inference will be located in the `output/spider/reranker` directory and saved in a folder using the following naming convention, 
`<benchmark_name>_<model_name>_<candidate_num>_<retrieval_model_name>_<reranker_embedding_name><reranker_model_name>` 

## Debug tips
For debugging SQLGenV2, you may use the `reranker_script_debug.py` file in the root directory;
For debugging Dialect Builder, you may use the `dialect_debug.py` file in the root directory;
For debugging any ranking models, you may use the `code_debug.py` file in the root directory.