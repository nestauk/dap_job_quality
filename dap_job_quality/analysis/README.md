# Evaluation

## Datasets

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

This will run the evaluation on 427 job adverts.

| JQ measure       | Number of job adverts |
| ---------------- | --------------------- |
| FLEX_HOURS       | 72                    |
| CONTRACT         | 70                    |
| L&D              | 38                    |
| CARING           | 37                    |
| CAREER           | 35                    |
| LEAVE            | 34                    |
| COMP             | 32                    |
| PERKS            | 31                    |
| HOURS            | 23                    |
| SOCIAL           | 22                    |
| HEALTH           | 15                    |
| REWARD           | 7                     |
| MISC             | 4                     |
| SENSE OF PURPOSE | 3                     |
| SHIFT            | 3                     |
| M_HEALTH         | 1                     |

This will output

1. the JQ predictions for each job advert in the evaluation data (`s3://open-jobs-lake/job_quality/outputs/evaluation/JQ_predictions_{DATE}.csv`)
2. the recall results for each job quality level and job quality measure within this level (`s3://open-jobs-lake/job_quality/outputs/evaluation/recall_results_{DATE}.csv`)

| JQ_measure_name  | JQ_level    | prop_ads_truth_and_pred | n_ads_truth | n_ads_pred | prop_ads_pred |
| ---------------- | ----------- | ----------------------- | ----------- | ---------- | ------------- |
| MISC             | subcategory | 0.75                    | 4           | 24         | 0.09          |
| COMP             | subcategory | 0.95                    | 20          | 185        | 0.70          |
| PERKS            | subcategory | 1.00                    | 19          | 166        | 0.63          |
| L&D              | subcategory | 1.00                    | 29          | 172        | 0.65          |
| LEAVE            | subcategory | 0.91                    | 23          | 126        | 0.48          |
| FLEX_HOURS       | subcategory | 1.00                    | 53          | 120        | 0.46          |
| CARING           | subcategory | 1.00                    | 28          | 115        | 0.44          |
| CAREER           | subcategory | 1.00                    | 33          | 101        | 0.38          |
| HOURS            | subcategory | 1.00                    | 15          | 209        | 0.79          |
| HEALTH           | subcategory | 1.00                    | 13          | 58         | 0.22          |
| CONTRACT         | subcategory | 0.76                    | 66          | 117        | 0.44          |
| SOCIAL           | subcategory | 1.00                    | 18          | 90         | 0.34          |
| REWARD           | subcategory | 1.00                    | 7           | 8          | 0.03          |
| SENSE OF PURPOSE | subcategory | 0.33                    | 3           | 7          | 0.03          |
| M_HEALTH         | subcategory | 1.00                    | 1           | 5          | 0.02          |
| SHIFT            | subcategory | 1.00                    | 3           | 15         | 0.06          |

An interpretation of these results for the FLEX_HOURS JQ measure is:

- 53 of the job adverts in the evaluation data had this measure.
- 100% of these 53 job adverts also had `FLEX_HOURS` predicted (recall).
- 120 of the job adverts in the evaluation data had this measure predicted - this is 46% of the evaluation job adverts.

Note: this gives us no indication of precision, so caution should be given especially when the `prop_ads_pred` value is close to 1.
