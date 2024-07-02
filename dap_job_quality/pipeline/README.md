# `find_job_quality.py`

This script runs the entire inference pipeline (extracting job quality-related sentences, chunking them into 4- to 6-grams and matching them to the taxonomy) for the EYP/similar occupations sample.

# `eyp/`

This folder contains scripts used for analysing a sample of job adverts for Early Years Practitioners and comparable occupations, in order to support Nesta's A Fairer Start mission.

See the subfolder `ngram_analysis/` and its readme for more info on how we extracted ngrams and matched them to the taxonomy.

# `prodigy/`

This folder contains scripts used to prepare data for labelling, and then label data using Prodigy.

# `sentence_classifier/`

The code in this folder trains a classifier to identify sentences that contain some element of job quality (eg salary, hours, other benefits). See the readme in that folder for more info.
