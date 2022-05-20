# Model Output Content and Format for GAR

The output **JSON** file contains the prediction results of the model for Spider `dev.json` (or `test.json`) with the following fields:
- `index`: the index referred in `dev.json`(or `test.json`)
- `db_id`: the database id for which the prediction is made
- `question`: the natural language question
- `inferred_query`: the predicted SQL query
- `inferred_query_with_marks`: the predicted SQL query with **low-confidence marks (@)**
- (optional) `gold_query`: the ground truth SQL query corresponding to the question
- (optional) `gold_query_toks_no_value`: the ground truth SQL query tokens corresponding to the question
- (optional) `exact`: if the inferred query matches the gold one

:bulb:
1. Make sure all SQL keywords (e.g. **SELECT, COUNT** etc.) in `inferred_query` and `inferred_query_with_marks` fieds are upper-cased;
2. Make sure all the values in SQL are represented with **single quotes**;
3. All the columns are represented with **table.column**.
```
{
    "index": 0, 
    "db_id": "concert_singer",
    "question": "How many singers do we have?",
    "inferred_query": "SELECT COUNT(*) FROM singer",
    "inferred_query_with_marks": "SELECT COUNT(*) FROM singer",
    "gold_query": "SELECT count(*) FROM singer",
    "gold_query_toks_no_value": [
        "select",
        "count",
        "(",
        "*",
        ")",
        "from",
        "singer"
    ],
    "exact": true
},
```
