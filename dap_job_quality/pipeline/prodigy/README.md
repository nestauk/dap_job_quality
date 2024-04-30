## Label job ads for dimensions of job quality

### Generate data to label

To generate data to label of size `ts` and save the data locally to an `.jsonl` file, run:

```
python dap_job_quality/pipeline/prodigy/make_labelled_data.py -ts 1000
```

If you would like to also save the data to s3, run:

```
python dap_job_quality/pipeline/prodigy/make_labelled_data.py -ts 1000 -s3 True
```

### Download BENEFITS model

To download the NER model that extracts `BENEFITS`, run:

```
aws s3 cp s3://open-jobs-lake/escoe_extension/outputs/models/ner_model/20230808/ ./outputs/models/ner_model/20230808/ --recursive
```

### Run prodigy instance

To install prodigy, run:

```
python -m pip install prodigy -f https://XXXX-XXXX-XXXX-XXXX@download.prodi.gy
```

where `XXXX-XXXX-XXXX-XXXX` is the prodigy license key.

To download data for labelling in prodigy (generated and saved to s3 using prep_data_for_labelling.py), run:

```
aws s3 cp <input s3 object URL>  ../../../inputs/labelling/<file_name>.jsonl

```

eg:

```
aws s3 cp s3://open-jobs-lake/job_quality/prodigy/labelling_data/20240418_ads_to_label_ts_500_random_seed_42.jsonl  ../../../inputs/labelling/20240418_ads_to_label_ts_500_random_seed_42.json
```

To run the custom prodigy instance (in the `prodigy` directory) for labelling benefits/ not benefits, navigate to dap_job_quality/pipeline/prodigy and run:

```
prodigy job_ad_sent_cat job_sentences_sample <input file> -F sentence_classifier_recipe.py

```

To export the labelled data locally, run:

```

prodigy db-out job_sentences_sample > ../../../inputs/labelled/<file_name>.jsonl

```

To save to S3, first export locally, then run:

```

aws s3 cp   ../../../inputs/labelling/<file_name>.jsonl <output s3 object URL>

```

### Labelling guidelines

We're currently building a binary classifier capture a broad range of job quality dimensions> The labellign guide is saved here (access for those internal to Nesta only) (https://docs.google.com/document/d/1c2chL5qUAZ244VwN_p9lv50p6uyWuN-imt36tP55tc0/edit?usp=drive_link)
