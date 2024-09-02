#!/bin/bash

[ -z "$1" ] && echo "First argument is the name of the dataset." && exit 1
DATASET_NAME="$1"

[ -z "$2" ] && echo "Second argument is the name of the SeqSeq model." && exit 1
MODEL_NAME="$2"

[ -z "$3" ] && echo "Third argument is the dataset train file." && exit 1
TRAIN_FILE="$3"

[ -z "$4" ] && echo "Fourth argument is the dataset dev file." && exit 1
DEV_FILE="$4"

[ -z "$5" ] && echo "Fifth argument is the model train output file." && exit 1
MODEL_TRAIN_OUTPUT_FILE="$5"

[ -z "$6" ] && echo "Sixth argument is the model dev output file." && exit 1
MODEL_DEV_OUTPUT_FILE="$6"

[ -z "$7" ] && echo "Seventh argument is the schema file of the dataset." && exit 1
TABLES_FILE="$7"

[ -z "$8" ] && echo "Eighth argument is the directory of the databases for the dataset." && exit 1
DB_DIR="$8"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
# DIR=$(pwd)

while true; do
    echo "Dataset name: $DATASET_NAME"
    echo "SeqSeq model name: $MODEL_NAME"
    echo "Train JSON file: $TRAIN_FILE"
    echo "Dev JSON file: $DEV_FILE"
    echo "The model train output JSON file: $MODEL_TRAIN_OUTPUT_FILE"
    echo "The model dev output JSON file.: $MODEL_DEV_OUTPUT_FILE"
    echo "Schema file of the dataset: $TABLES_FILE"
    echo "Databases directory of the dataset: $DB_DIR"
    read -p "Is this ok [y/n] ? " yn
    case $yn in
        [Yy]* ) break;;
        [Nn]* ) exit 0;;
        * ) echo "Please answer y or n.";;
    esac
done

echo "=================================================================="
echo "ACTION REPORT: Training pipeline starts ......"
output=`python3 -m configs.get_config_for_bash $DATASET_NAME`
RETRIEVAL_EMBEDDING_MODEL_NAME=$(cut -d'@' -f1 <<< "$output")
RERANKER_EMBEDDING_MODEL_NAME=$(cut -d'@' -f2 <<< "$output")
RERANKER_MODEL_NAME=$(cut -d'@' -f3 <<< "$output")
SEMSIMILARITY_TRIPLE_DATA_GZ_FILE=$(cut -d'@' -f4 <<< "$output")
RETRIEVAL_MODEL_DIR=$(cut -d'@' -f5 <<< "$output")
RERANKER_TRAIN_DATA_FILE=$(cut -d'@' -f6 <<< "$output")
RERANKER_DEV_DATA_FILE=$(cut -d'@' -f7 <<< "$output")
RERANKER_CONFIG_FILE=$(cut -d'@' -f8 <<< "$output")
RERANKER_MODEL_DIR=$(cut -d'@' -f9 <<< "$output")
PRED_TOPK_FILE_NAME=$(cut -d'@' -f10 <<< "$output")
TRIAL_KNOB=$(cut -d'@' -f11 <<< "$output")
CANDIDATE_NUM=$(cut -d'@' -f12 <<< "$output")
REWRITE_FLAG=$(cut -d'@' -f13 <<< "$output")
OVERWRITE_FALG=$(cut -d'@' -f14 <<< "$output")
MODE=$(cut -d'@' -f15 <<< "$output")
DEBUG=$(cut -d'@' -f16 <<< "$output")
echo "&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&"
echo "python3 -m datagen.generalization_script $DATASET_NAME $MODEL_NAME $TRAIN_FILE $MODEL_TRAIN_OUTPUT_FILE $TABLES_FILE $DB_DIR $TRIAL_KNOB $REWRITE $OVERWRITE $MODE"
echo "&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&"


RETRIEVAL_MODEL_DIR=$RETRIEVAL_MODEL_DIR/$RETRIEVAL_EMBEDDING_MODEL_NAME
RERANKER_MODEL_DIR=$RERANKER_MODEL_DIR/$RERANKER_MODEL_NAME\_$RERANKER_EMBEDDING_MODEL_NAME

# Generalize SQL-Dialects for the train and dev data
echo "ACTION REPORT: Generalize the sqls(and dialects) for the train data "
python3 -m datagen.generalization_script $DATASET_NAME $MODEL_NAME $TRAIN_FILE $MODEL_TRAIN_OUTPUT_FILE \
$TABLES_FILE $DB_DIR $TRIAL_KNOB $REWRITE $OVERWRITE $MODE
echo "RESULT REPORT: Train data generalization is done!"
echo "=================================================================="
python3 -m datagen.generalization_script $DATASET_NAME $MODEL_NAME $DEV_FILE $MODEL_DEV_OUTPUT_FILE \
$TABLES_FILE $DB_DIR $TRIAL_KNOB $REWRITE_FLAG $OVERWRITE_FALG $MODE
echo "RESULT REPORT: Dev data generalization is done!"
echo "=================================================================="

if [ ! -f $SEMSIMILARITY_TRIPLE_DATA_GZ_FILE ]; then
    echo "ACTION REPORT: Generate sentence embedder fine-tune data ......"
    python3 -m datagen.retrieval_model_train_script $DATASET_NAME $TRAIN_FILE $DEV_FILE \
    $MODEL_TRAIN_OUTPUT_FILE $MODEL_DEV_OUTPUT_FILE $TABLES_FILE $DB_DIR
    echo "RESULT REPORT: The retrieval model fine-tune data is ready now!"
    echo "=================================================================="
else
    echo "The retrieval model fine-tune data has alredy existed!"
    echo "=================================================================="
fi

if [ ! -d $RETRIEVAL_MODEL_DIR ]; then
    echo "ACTION REPORT: Start to fine-tune the retrieval model ......"
    python3 -m allenmodels.models.semantic_matcher.sentence_transformers.sentence_embedder \
    $RETRIEVAL_EMBEDDING_MODEL_NAME $RETRIEVAL_MODEL_DIR $DATASET_NAME
    echo "RESULT REPORT: The retrieval model fine-tune complete!"
    echo "=================================================================="
else
    echo "The retrieval model has already existed!"
    echo "=================================================================="
    read -p "Do you want to continue? " yn
    case $yn in
        [Yy]* ) ;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
fi

echo "##############################################################################"
echo "python3 -m datagen.reranker_script $DATASET_NAME $MODEL_NAME $RETRIEVAL_EMBEDDING_MODEL_NAME $TRAIN_FILE $MODEL_TRAIN_OUTPUT_FILE $TABLES_FILE $DB_DIR $CANDIDATE_NUM $TRIAL_KNOB $REWRITE_FLAG $OVERWRITE_FALG $MODE $DEBUG $RERANKER_TRAIN_DATA_FILE"
echo "python3 -m datagen.reranker_script $DATASET_NAME $MODEL_NAME $RETRIEVAL_EMBEDDING_MODEL_NAME $DEV_FILE $MODEL_DEV_OUTPUT_FILE $TABLES_FILE $DB_DIR $CANDIDATE_NUM $TRIAL_KNOB $REWRITE_FLAG $OVERWRITE_FALG $MODE $DEBUG $RERANKER_DEV_DATA_FILE"
echo "##############################################################################"

if [ ! -f $RERANKER_TRAIN_DATA_FILE ]; then
    echo "ACTION REPORT: Generate the re-ranking model's train data ......"
    echo "python3 -m datagen.reranker_script $DATASET_NAME $MODEL_NAME $RETRIEVAL_EMBEDDING_MODEL_NAME $TRAIN_FILE $MODEL_TRAIN_OUTPUT_FILE $TABLES_FILE $DB_DIR $CANDIDATE_NUM $TRIAL_KNOB $REWRITE_FLAG $OVERWRITE_FALG $MODE $DEBUG $RERANKER_TRAIN_DATA_FILE"
    python3 -m datagen.reranker_script $DATASET_NAME $MODEL_NAME \
    $RETRIEVAL_EMBEDDING_MODEL_NAME $TRAIN_FILE $MODEL_TRAIN_OUTPUT_FILE $TABLES_FILE $DB_DIR \
    $CANDIDATE_NUM $TRIAL_KNOB $REWRITE_FLAG $OVERWRITE_FALG $MODE $DEBUG $RERANKER_TRAIN_DATA_FILE
    echo "RESULT REPORT: The re-ranking model's train data is ready now!"
    echo "=================================================================="
else
    echo "The re-ranking model's train data exists!"
    echo "=================================================================="
fi

if [ ! -f $RERANKER_DEV_DATA_FILE ]; then
    echo "ACTION REPORT: Generate the re-ranking model's dev data ......"
    python3 -m datagen.reranker_script $DATASET_NAME $MODEL_NAME \
    $RETRIEVAL_EMBEDDING_MODEL_NAME $DEV_FILE $MODEL_DEV_OUTPUT_FILE $TABLES_FILE $DB_DIR \
    $CANDIDATE_NUM $TRIAL_KNOB $REWRITE_FLAG $OVERWRITE_FALG $MODE $DEBUG $RERANKER_DEV_DATA_FILE
    echo "RESULT REPORT: The re-ranking model's dev data is ready now!"
    echo "=================================================================="
else
    echo "The re-ranking model's dev data exists!"
    echo "=================================================================="
    read -p "Do you want to continue? " yn
    case $yn in
        [Yy]* ) ;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
fi

echo "&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&"
echo "python3 -m configs.update_config $RERANKER_CONFIG_FILE $RERANKER_EMBEDDING_MODEL_NAME $RERANKER_TRAIN_DATA_FILE $RERANKER_DEV_DATA_FILE"
echo "&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&"
echo "allennlp train $RERANKER_CONFIG_FILE -s $RERANKER_MODEL_DIR --include-package allenmodels.dataset_readers.listwise_pair_reader --include-package allenmodels.models.semantic_matcher.listwise_pair_ranker"
echo "&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&"

if [ ! -d $RERANKER_MODEL_DIR ]; then
    echo "=================================================================="
    echo "ACTION REPORT: Change the embedding model name in the config file..."
    python3 -m configs.update_config "$RERANKER_CONFIG_FILE" \
    "$RERANKER_EMBEDDING_MODEL_NAME" "$RERANKER_TRAIN_DATA_FILE" "$RERANKER_DEV_DATA_FILE"
    echo "RESULT REPORT: The config file of the re-ranking model has been updated!"
    echo "ACTION REPORT: Start to train the re-ranking model ......"
    allennlp train "$RERANKER_CONFIG_FILE" -s "$RERANKER_MODEL_DIR" \
    --include-package allenmodels.dataset_readers.listwise_pair_reader \
    --include-package allenmodels.models.semantic_matcher.listwise_pair_ranker || exit $?
    echo "RESULT REPORT: Train re-ranking model complete!"
else
    echo "The re-ranking model has already existed!"
    echo "=================================================================="
    read -p "Do you want to continue? " yn
    case $yn in
        [Yy]* ) ;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
fi
echo "Train pipeline completed!"
