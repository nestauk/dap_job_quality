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

To run the custom prodigy instance (in the `prodigy` directory), run:

```
prodigy benefits_classification job_quality_annotated \
    ./labelled_data/YYYYMMDD_ads_to_label_ts_1000_random_seed_42.jsonl \
    -F custom_recipe.py
```

To save the outputs of the labelling exercise locally and to s3, run:

```
prodigy db-out job_quality_annotated > ./labelled_data/20240117_ads_labelled.jsonl
aws s3 cp ./labelled_data/20240117_ads_labelled.jsonl s3://open-jobs-lake/job_quality/prodigy/labelled_data/20240117_ads_labelled.jsonl
```

### Labelling guidelines

We're trying to see **which dimensions of job quality we can extract from job ads**. We've mapped out the different possible dimensions and would like to assess the feasibility of extracting these dimensions from job ads. Please [refer to the tentative feasibility matrix for entity definitions.](https://docs.google.com/document/d/1b57AuyA00FdNo1AkiB4Ne_KhUBi0uyPUuQd9bBQsC4Q/edit?usp=sharing)
