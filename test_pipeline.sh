#!/bin/bash

[ -z "$1" ] && echo "First argument is the name of the NLIDB benchmark." && exit 1
DATASET_NAME="$1"

[ -z "$2" ] && echo "Second argument is the name of the seq2seq model." && exit 1
MODEL_NAME="$2"

[ -z "$3" ] && echo "Third argument is the outout file of the seq2seq model." && exit 1
MODEL_OUTPUT_FILE="$3"

[ -z "$4" ] && echo "Fourth argument is the test json file." && exit 1
TEST_FILE="$4"

[ -z "$5" ] && echo "Fifth argument is dev gold sql file used for evaluation. " && exit 1
GOLD_SQL_FILE="$5"

[ -z "$6" ] && echo "Sixth argument is the schema file." && exit 1
TABLES_FILE="$6"

[ -z "$7" ] && echo "Seventh argument is the directory of the databases." && exit 1
DB_DIR="$7"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
# DIR=$(pwd)

while true; do
    echo "Dataset name: $DATASET_NAME"
    echo "SeqSeq model name: $MODEL_NAME"
    echo "Seq2Seq model output JSON file: $MODEL_OUTPUT_FILE"
    echo "Test JSON file: $TEST_FILE"
    echo "Gold SQL TXT file: $GOLD_SQL_FILE"
    echo "Dataset schema JSON file: $TABLES_FILE"
    echo "SQLite databases' directory: $DB_DIR"
    read -p "Is this ok [y/n] ? " yn
    case $yn in
        [Yy]* ) break;;
        [Nn]* ) exit 0;;
        * ) echo "Please answer y or n.";;
    esac
done

echo "=================================================================="
echo "ACTION REPORT: Testing pipeline starts ......"
output=`python3 -m configs.get_config_for_test_bash $DATASET_NAME`
OUTPUT_DIR_RERANKER=$(cut -d'@' -f1 <<< "$output")
RETRIEVAL_EMBEDDING_MODEL_NAME=$(cut -d'@' -f2 <<< "$output")
RERANKER_MODEL_DIR=$(cut -d'@' -f3 <<< "$output")
RERANKER_EMBEDDING_MODEL_NAME=$(cut -d'@' -f4 <<< "$output")
RERANKER_MODEL_NAME=$(cut -d'@' -f5 <<< "$output")
RERANKER_INPUT_FILE_NAME=$(cut -d'@' -f6 <<< "$output")
PRED_FILE_NAME=$(cut -d'@' -f7 <<< "$output")
RERANKER_MISS_FILE_NAME=$(cut -d'@' -f8 <<< "$output")
MODEL_TAR_GZ=$(cut -d'@' -f9 <<< "$output")
PRED_TOPK_FILE_NAME=$(cut -d'@' -f10 <<< "$output")
TRIAL_KNOB=$(cut -d'@' -f11 <<< "$output")
CANDIDATE_NUM=$(cut -d'@' -f12 <<< "$output")
REWRITE_FLAG=$(cut -d'@' -f13 <<< "$output")
OVERWRITE_FALG=$(cut -d'@' -f14 <<< "$output")
MODE=$(cut -d'@' -f15 <<< "$output")
DEBUG=$(cut -d'@' -f16 <<< "$output")

EXPERIMENT_DIR_NAME=$OUTPUT_DIR_RERANKER/$DATASET_NAME\_$MODEL_NAME\_$CANDIDATE_NUM\_$RETRIEVAL_EMBEDDING_MODEL_NAME\_$RERANKER_EMBEDDING_MODEL_NAME\_$RERANKER_MODEL_NAME
if [ ! -d $EXPERIMENT_DIR_NAME ]; then
    mkdir -p $EXPERIMENT_DIR_NAME
fi
RERANKER_INPUT_FILE=$EXPERIMENT_DIR_NAME/$RERANKER_INPUT_FILE_NAME
RERANKER_MODEL_FILE=$RERANKER_MODEL_DIR/$RERANKER_MODEL_NAME\_$RERANKER_EMBEDDING_MODEL_NAME/$MODEL_TAR_GZ
RERANKER_MODEL_OUTPUT_FILE=$EXPERIMENT_DIR_NAME/$PRED_FILE_NAME
RERANKER_MODEL_OUTPUT_TOPK_FILE=$EXPERIMENT_DIR_NAME/$PRED_TOPK_FILE_NAME
RERANKER_MODEL_OUTPUT_SQL_FILE=${RERANKER_MODEL_OUTPUT_FILE/.txt/_sql.txt}
RERANKER_MODEL_OUTPUT_TOPK_SQL_FILE=${RERANKER_MODEL_OUTPUT_FILE/.txt/_sql_topk.txt}
EVALUATE_OUTPUT_FILE=${RERANKER_MODEL_OUTPUT_FILE/.txt/_evaluate.txt}
VALUE_FILTERED_OUTPUT_SQL_FILE=${RERANKER_MODEL_OUTPUT_FILE/.txt/_sql_value_filtered.txt}
VALUE_FILTERED_OUTPUT_TOPK_SQL_FILE=${RERANKER_MODEL_OUTPUT_FILE/.txt/_sql_topk_value_filtered.txt}
FINAL_OUTPUT_FILE="${EXPERIMENT_DIR_NAME}/pred_final.txt"

# Generalize SQL-Dialects for the test data
echo "ACTION REPORT: Generalize the sqls(and dialects) for the test data "
python3 -m datagen.generalization_script $DATASET_NAME $MODEL_NAME $TEST_FILE $MODEL_OUTPUT_FILE \
$TABLES_FILE $DB_DIR $TRIAL_KNOB $REWRITE_FLAG $OVERWRITE_FALG $MODE
echo "RESULT REPORT: Test data generalization is done!"
echo "=================================================================="

# Generate the input data for the re-ranking model 
if [ ! -f $RERANKER_INPUT_FILE ]; then
    echo "ACTION REPORT: Generate re-ranking model's input data to $RERANKER_INPUT_FILE"
    python3 -m datagen.reranker_script $DATASET_NAME $MODEL_NAME $RETRIEVAL_EMBEDDING_MODEL_NAME \
    $TEST_FILE $MODEL_OUTPUT_FILE $TABLES_FILE $DB_DIR \
    $CANDIDATE_NUM $TRIAL_KNOB $REWRITE_FLAG $OVERWRITE_FALG $MODE $DEBUG $RERANKER_INPUT_FILE
    echo "RESULT REPORT: The input data of the re-ranking model is ready now!"
    echo "=================================================================="
else
    echo "The input data of the re-ranking model has already existed!"
    echo "=================================================================="
    read -p "Do you want to continue? " yn
    case $yn in
        [Yy]* ) ;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
fi

# Inference for top-1
if [ -f $RERANKER_MODEL_FILE -a ! -f $RERANKER_MODEL_OUTPUT_FILE ]; then
    echo "ACTION REPORT: Start to infer the top-1 results using the re-ranking model $RERANKER_MODEL_FILE"
    allennlp predict "$RERANKER_MODEL_FILE" "$RERANKER_INPUT_FILE" \
    --output-file "$RERANKER_MODEL_OUTPUT_FILE" \
    --file-friendly-logging --silent --predictor listwise-ranker --use-dataset-reader --cuda-device 0 \
    --include-package allenmodels.dataset_readers.listwise_pair_reader \
    --include-package allenmodels.models.semantic_matcher.listwise_pair_ranker \
    --include-package allenmodels.predictors.ranker_predictor || exit $?
    echo "RESULT REPORT: Re-ranking model inference (top-1) complete!"
    echo "=================================================================="
else
    echo "Re-ranking model $RERANKER_MODEL_FILE does not exist or $RERANKER_MODEL_OUTPUT_FILE exists."
    echo "=================================================================="
    read -p "Do you want to continue? " yn
    case $yn in
        [Yy]* ) ;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
fi

# Evaluate re-ranker model
if [ -f $RERANKER_MODEL_OUTPUT_FILE -a ! -f $RERANKER_MODEL_OUTPUT_SQL_FILE ]; then
    echo "ACTION REPORT: Start to evaluate the re-ranking model (top-1 results) and generate top-1 sql file......"
    python3 -m eval_scripts.reranker_evaluate $TABLES_FILE $DB_DIR $RERANKER_MODEL_OUTPUT_FILE \
    $RERANKER_INPUT_FILE $EXPERIMENT_DIR_NAME
    echo "RESULT REPORT: Re-ranking model evaluation complete!"
    echo "=================================================================="
else
    echo "The output of re-ranking model does not exist or the top-1 SQL file exists"
    echo "=================================================================="
    read -p "Do you want to continue? " yn
    case $yn in
        [Yy]* ) ;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
fi

# Inference for top-k
if [ -f $RERANKER_MODEL_FILE -a ! -f $RERANKER_MODEL_OUTPUT_TOPK_FILE ]; then
    echo "ACTION REPORT: Start to infer the top-k results using the re-ranking model $RERANKER_MODEL_FILE"
    allennlp predict "$RERANKER_MODEL_FILE" "$RERANKER_INPUT_FILE" \
    --output-file "$RERANKER_MODEL_OUTPUT_TOPK_FILE" \
    --file-friendly-logging --silent --predictor listwise-ranker --use-dataset-reader --cuda-device 0 \
     --include-package allenmodels.dataset_readers.listwise_pair_reader \
     --include-package allenmodels.models.semantic_matcher.listwise_pair_ranker \
     --include-package allenmodels.predictors.ranker_predictor_topk || exit $?
    echo "RESULT REPORT: Re-ranking model inference (top-k) complete!"
    echo "=================================================================="
else
    echo "Re-ranking model $RERANKER_MODEL_FILE does not exist or $RERANKER_MODEL_OUTPUT_TOPK_FILE exists."
    echo "=================================================================="
    read -p "Do you want to continue? " yn
    case $yn in
        [Yy]* ) ;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
fi


# Evaluate for top-k
if [ -f $RERANKER_MODEL_OUTPUT_TOPK_FILE -a ! -f $RERANKER_MODEL_OUTPUT_TOPK_SQL_FILE ]; then
    echo "ACTION REPORT: Start to evaluate the re-ranking model (top-k results) and generate top-k sql file......"
    python3 -m eval_scripts.reranker_evaluate_topk $TABLES_FILE $DB_DIR \
    $RERANKER_MODEL_OUTPUT_TOPK_FILE $RERANKER_INPUT_FILE $EXPERIMENT_DIR_NAME
    echo "RESULT REPORT: Top-k result generate complete!"
    echo "=================================================================="
else
    echo "The top-k output of the re-ranking model does not exist or top-k SQL file exists"
    echo "=================================================================="
    read -p "Do you want to continue? " yn
    case $yn in
        [Yy]* ) ;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
fi


# Value filtered
if [ -f $RERANKER_MODEL_OUTPUT_SQL_FILE -a ! -f $VALUE_FILTERED_OUTPUT_TOPK_SQL_FILE ]; then
    echo "Value filter stage starting..."
    python3 -m value_mathcing.candidate_filter_top10 "$TEST_FILE" "$RERANKER_INPUT_FILE" \
    "$RERANKER_MODEL_OUTPUT_TOPK_SQL_FILE" "$TABLES_FILE" "$DB_DIR" \
    "$VALUE_FILTERED_OUTPUT_SQL_FILE" "$VALUE_FILTERED_OUTPUT_TOPK_SQL_FILE"
    echo "Value filter result saved in $VALUE_FILTERED_OUTPUT_SQL_FILE"
    echo "Value filter top-k result saved in $VALUE_FILTERED_OUTPUT_TOPK_SQL_FILE"
    echo "Value filter stage complete!"
    echo "=================================================================="
else
    echo "Value filter result exist!"
    echo "=================================================================="
    read -p "Do you want to continue? " yn
    case $yn in
        [Yy]* ) ;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
fi


# Evaluate for value filtered
if [ -f $VALUE_FILTERED_OUTPUT_TOPK_SQL_FILE ]; then
    echo "Value filter evaluation starting..."
    python3 -m value_mathcing.value_matching_evaluate "$TABLES_FILE" "$DB_DIR" "$RERANKER_INPUT_FILE" \
    "$VALUE_FILTERED_OUTPUT_TOPK_SQL_FILE" "$EXPERIMENT_DIR_NAME"
    echo "Value filter evaluation complete!"
    echo "=================================================================="
else
    echo "Value filter result exist!"
    echo "=================================================================="
    read -p "Do you want to continue? " yn
    case $yn in
        [Yy]* ) ;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
fi


# Final Evaluation
if [ -f $VALUE_FILTERED_OUTPUT_SQL_FILE -a ! -f $EVALUATE_OUTPUT_FILE ]; then
    python3 -m datagen.postprocess_script "$RERANKER_INPUT_FILE" "$VALUE_FILTERED_OUTPUT_SQL_FILE" \
    "$MODEL_OUTPUT_FILE" "$FINAL_OUTPUT_FILE"
    echo "Start evaluating the inference results using Spider evalution script"
    python3 -m spider_utils.evaluation.evaluate --gold "$GOLD_SQL_FILE" --pred "$FINAL_OUTPUT_FILE" \
    --etype "match" --db "$DB_DIR" --table "$TABLES_FILE" \
    --candidates "$VALUE_FILTERED_OUTPUT_TOPK_SQL_FILE" > "$EVALUATE_OUTPUT_FILE"
    echo "Final evaluation Finished!"
    echo "Final evaluation result saved in $EVALUATE_OUTPUT_FILE"
    echo "=================================================================="
else
    echo "Final evaluation result exist!"
    echo "=================================================================="
    exit
fi
echo "Test Pipeline completed!"