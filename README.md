# A Confidence-based Knowledge Integration Framework for Cross-Domain Table Question Answering

The official repository contains the code and models for our paper [A Confidence-based Knowledge Integration Framework for Cross-Domain Table Question Answering](https://ieeexplore.ieee.org/document/xxx). *2024 Knowledge-Based Systems Journal (KBS).*

<p align="center">
   <a href="https://github.com/kaimary/CKIF/blob/main/LICENSE">
        <img alt="license" src="https://img.shields.io/github/license/kaimary/CKIF.svg?color=blue">
   </a>
   <a href="https://github.com/kaimary/CKIF/stargazers">
       <img alt="stars" src="https://img.shields.io/github/stars/kaimary/CKIF" />
  	</a>
  	<a href="https://github.com/kaimary/CKIF/network/members">
       <img alt="FORK" src="https://img.shields.io/github/forks/kaimary/CKIF?color=FF8000" />
  	</a>
    <a href="https://github.com/kaimary/CKIF/issues">
      <img alt="Issues" src="https://img.shields.io/github/issues/kaimary/CKIF?color=0088ff"/>
    </a>
    <br />
</p>

## Setup

Download the Spider dataset and put the data in `datasets` folder in the root directory

## Postprocessing on model raw output
Before making inference with GAR, we need to format the outputs of any Seq2Seq models first. Please check the README in the `model_output_postprocess` folder for the details.

The postprocessing of model `GAP`, `RAT-SQL` and `BRIDGE` has been added in the `model_output_postprocess.py` file in the root directory.

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

### Output files
All the outputs of the inference will be located in the `output/spider/reranker` directory and saved in a folder using the following naming convention, 
`<benchmark_name>_<model_name>_<candidate_num>_<retrieval_model_name>_<reranker_embedding_name><reranker_model_name>` 

## Debug tips
- For debugging SQLGenV2, you may use the `reranker_script_debug.py` file in the root directory;
- For debugging Dialect Builder, you may use the `dialect_debug.py` file in the root directory;
- For debugging any ranking models, you may use the `code_debug.py` file in the root directory.
