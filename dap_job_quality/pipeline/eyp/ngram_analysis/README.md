The scripts in this folder extract Ngrams from JQ-related sentences in the EYP/sim. occ. sample, match these to the taxonomy, and save the results.

At this stage, we do not use a threshold for cosine similarity because the resulting data is used to evaluate match quality and hence determine what the cosine similarity threshold should be.

The final run of this pipeline (using a cosine similarity threshold) is in `pipeline/find_job_quality.py`.

The scripts run in this order:

1. `get_embeddings.py`: embeds the adverts from the sample using [jobbert](https://huggingface.co/jjzha/jobbert-base-cased). These embeddings are generated and saved separately to the rest of the process because they take up so much memory.

2. `extract_jq_sentences.py`: the embeddings generated at the previous stage are reduced using a pre-fit PCA, and predictions are generated using a logistic regression classifier (the code that trains these is in `pipeline/sentence_classifier/`). In order to maximise recall, we use a threshold of 0.3 instead of the usual 0.5 with the logistic regression model. Sentences that are predicted to relate to job quality are filtered and saved.

3. `get_ngrams_and_matches.py`: further cleaning is done to the sentences that are predicted to relate to job quality. Any sentences fewer that 6 words are split into 4-grams.These 4-grams are then embedded with [all-MiniLM-L6-v2](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2), as are target phrases from the [taxonomy](s3://open-jobs-lake/job_quality/keywords/keyword_lookup - v6.csv). Cosine similarity between the two is computed. For a single target phrase, per sentence, the Ngram with the highest cosine similarity is saved (because lots of overlapping Ngrams will match to the same target phrase, but we just want to keep the one that is the best match).
