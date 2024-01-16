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

Alternatively, you can just download the data to label from s3 using the `aws cli`:

```
aws s3 cp s3://open-jobs-lake/job_quality/prodigy/labelled_data/20240116_ads_to_label.jsonl ./dap_job_quality/pipeline/prodigy/labelled_data/20241116_ads_to_label.jsonl
```

### Download benefits model

To download the NER model that extracts `BENEFITS`, run:

```
aws s3 cp s3://open-jobs-lake/escoe_extension/outputs/models/ner_model/20230808/ ./outputs/models/ner_model/20230808/ --recursive
```

### Run prodigy instance

To avoid any environment conflicts, it would be best to create a new prodigy environment, [install Prodigy](https://prodi.gy/docs/install) in your prodigy environment and a few additional Python dependencies:

```
conda create --name prodigy_env pip python=3.10
conda activate prodigy_env
python -m pip install prodigy -f https://XXXX-XXXX-XXXX-XXXX@download.prodi.gy
python -m pip install -r prodigy_requirements.txt #install additional libraries
```

To run the custom prodigy instance, run:

```
prodigy benefits_ner_classification job_quality_annotated \
    dap_job_quality/pipeline/prodigy/labelled_data/20240116_ads_to_label.jsonl \
    -F dap_prinz_green_jobs/pipeline/prodigy/custom_recipe.py
```

To save the outputs of the labelling exercise locally and to s3, run:

```
prodigy db-out job_quality_annotated > dap_job_quality/pipeline/prodigy/labelled_data/20240116_ads_labelled.jsonl

aws s3 cp dap_job_quality/pipeline/prodigy/labelled_data/20240116_ads_labelled.jsonl s3://open-jobs-lake/job_quality/prodigy/labelled_data/20240116_ads_labelled.jsonl
```
