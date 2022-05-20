import os
import sys
dir_path = os.getcwd() 

config_vars = {}
with open(dir_path+'/configs/config.py', 'r') as f:
    for line in f:
        if '=' in line:
            k,v = line.split('=', 1)
            k = k.strip()
            if k in ["RETRIEVAL_MODEL_EMBEDDING_DIMENSION", "RETRIEVAL_EMBEDDING_MODEL_NAME", \
                "RERANKER_EMBEDDING_MODEL_NAME", "RERANKER_MODEL_NAME", "CANDIDATE_NUM", \
                "RERANKER_INPUT_FILE_NAME", "PRED_FILE_NAME", "PRED_TOPK_FILE_NAME", \
                "CANDIDATE_MISS_FILE_NAME", "SQL_MISS_FILE_NAME", "RERANKER_MISS_FILE_NAME", \
                "MODEL_TAR_GZ", 'TRIAL_KNOB', 'REWRITE_FLAG', 'OVERWRITE_FLAG', 'MODE', 'DEBUG']:
                config_vars[k] = v.strip().strip("'")
            elif k in ['OUTPUT_DIR_RERANKER', 'RERANKER_MODEL_DIR']:
                config_vars[k] = dir_path + v.format(sys.argv[1]).strip().strip("'")
            else:
                config_vars[k] = dir_path + v.strip().strip("'")
#print(f"config_vars:{config_vars}")
print(f"{config_vars['OUTPUT_DIR_RERANKER']}@{config_vars['RETRIEVAL_EMBEDDING_MODEL_NAME']}@"
    f"{config_vars['RERANKER_MODEL_DIR']}@{config_vars['RERANKER_EMBEDDING_MODEL_NAME']}@"
    f"{config_vars['RERANKER_MODEL_NAME']}@{config_vars['RERANKER_INPUT_FILE_NAME']}@"
    f"{config_vars['PRED_FILE_NAME']}@{config_vars['RERANKER_MISS_FILE_NAME']}@"
    f"{config_vars['MODEL_TAR_GZ']}@{config_vars['PRED_TOPK_FILE_NAME']}@{config_vars['TRIAL_KNOB']}@"
    f"{config_vars['CANDIDATE_NUM']}@{config_vars['REWRITE_FLAG']}@{config_vars['OVERWRITE_FLAG']}@"
    f"{config_vars['MODE']}@{config_vars['DEBUG']}")
