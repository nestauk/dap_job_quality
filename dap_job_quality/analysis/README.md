# Evaluation

## Datasets

### Evaluation data

We ran

```
python dap_job_quality/analysis/full_pipeline_evaluation/prepare_sample.py

```

to create a sample of the job adverts in the hold-on test set (for the job quality sentence classifier), plus some extra job adverts from categories key to the early years analysis.

We then manually labelled 100 of these with whether each of the job quality measures were in them or not. This manually labelled dataset is in `s3://open-jobs-lake/job_quality/outputs/evaluation/evaluation_data_12_08_24_per_sentence_evaluation.csv`.

### Random sample

We used a version of the mapping algorithm to predict job quality measures for a sample of job adverts. These predictions were manually rated for their quality. We use the mappings which were given a 'good' rating as part of our evaluation dataset.

The live version of this data is [here](https://docs.google.com/spreadsheets/d/1t3SBXdG04f9IJuFRzVgS6ISGQ4_yGKwf0BgWRdUCdsQ/edit?gid=1578076900#gid=1578076900) and a static version downloaded on 08/08/2024 is on S3 [here](s3://open-jobs-lake/job_quality/sentence_classifier/inputs/labelled/mapping_evaluation/sample_10000_evaluation_080824.csv).

Since this dataset was created using an old version of the algorithm, we can see these results as true positives, but accept they might not be the full set of true positives.

### Contract types

As a separate piece of analysis we wanted to see whether the original contract type the job advert board used was good enough for evaluation. This involved labelling the contract type for a random sample of job adverts. The original contract type field was not good enough for evaluation, however in this process we labelled 40 job adverts with the correct contract type - and thus can use these in our evaluation too.

The live version of this data is [here](https://docs.google.com/spreadsheets/d/1lBOOAfFxXzp-dadALaMuBXBCrmWPZfE5aXYzetwQ0rg/edit?gid=125603757#gid=125603757) and a static version downloaded on 08/08/2024 is on S3 [here](s3://open-jobs-lake/job_quality/sentence_classifier/inputs/labelled/mapping_evaluation/contract_type_sample_080824.csv).

Since we haven't labelled the other job quality measures for the contract type dataset - we can only use this to evaluate the recall using this.

## Script

To run the evaluation run

```
python dap_job_quality/analysis/full_pipeline_evaluation/evaluate_pipeline.py
```

This will run the evaluation on the manually labelled 100 job adverts. The number of each JQ measure labelled is as following:

| JQ measure           | Number of job adverts |
| -------------------- | --------------------- |
| L&D                  | 48                    |
| CAREER               | 25                    |
| HOURS                | 59                    |
| FLEX_HOURS           | 35                    |
| SHIFT                | 17                    |
| LOC                  | 36                    |
| FLEX_LOC             | 28                    |
| CONTRACT             | 39                    |
| LEAVE                | 30                    |
| COMP                 | 86                    |
| PERKS                | 55                    |
| CARING               | 8                     |
| DISABILITY           | 2                     |
| HEALTH               | 7                     |
| M_HEALTH             | 4                     |
| SPONSORSHIP          | 2                     |
| REWARD               | 3                     |
| MISC                 | 13                    |
| AUTONOMY             | 1                     |
| SENSE OF PURPOSE     | 4                     |
| SOCIAL               | 22                    |
| VOICE REPRESENTATION | 0                     |

This will output

1. the JQ predictions and truth for each job advert in the evaluation data (`s3://open-jobs-lake/job_quality/outputs/evaluation/JQ_prediction_errors_{DATE}.csv`)
2. the evaluation results for each job quality level and job quality measure within this level (`s3://open-jobs-lake/job_quality/outputs/evaluation/JQ_evaluation_results_{DATE}.csv`)

| jq_measure           | precision | recall | f1-score | support |
| -------------------- | --------- | ------ | -------- | ------- |
| L&D                  | 0.875     | 0.875  | 0.875    | 48.0    |
| CAREER               | 0.750     | 0.720  | 0.735    | 25.0    |
| HOURS                | 0.855     | 1.000  | 0.922    | 59.0    |
| FLEX_HOURS           | 0.612     | 0.857  | 0.714    | 35.0    |
| SHIFT                | 0.714     | 0.294  | 0.417    | 17.0    |
| LOC                  | 0.619     | 0.342  | 0.441    | 38.0    |
| FLEX_LOC             | 0.720     | 0.900  | 0.800    | 20.0    |
| CONTRACT             | 0.781     | 0.641  | 0.704    | 39.0    |
| LEAVE                | 0.652     | 1.000  | 0.789    | 30.0    |
| COMP                 | 1.000     | 0.860  | 0.925    | 86.0    |
| PERKS                | 0.883     | 0.964  | 0.922    | 55.0    |
| CARING               | 0.381     | 1.000  | 0.552    | 8.0     |
| DISABILITY           | 0.000     | 0.000  | 0.000    | 2.0     |
| HEALTH               | 0.545     | 0.857  | 0.667    | 7.0     |
| M_HEALTH             | 0.400     | 0.500  | 0.444    | 4.0     |
| SPONSORSHIP          | 1.000     | 1.000  | 1.000    | 2.0     |
| REWARD               | 0.000     | 0.000  | 0.000    | 3.0     |
| MISC                 | 0.300     | 0.231  | 0.261    | 13.0    |
| AUTONOMY             | 0.500     | 1.000  | 0.667    | 1.0     |
| SENSE OF PURPOSE     | 0.000     | 0.000  | 0.000    | 4.0     |
| SOCIAL               | 0.320     | 0.364  | 0.340    | 22.0    |
| VOICE REPRESENTATION | 0.000     | 0.000  | 0.000    | 0.0     |
