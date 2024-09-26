# Evaluation

## Pipeline performance

The end-to-end pipeline was evaluated against the hold-out test set, resulting in the following metrics:

| Job quality measure  | precision | recall | f1-score | support |
| -------------------- | --------- | ------ | -------- | ------- |
| L&D                  | 0.917     | 0.846  | 0.880    | 52.0    |
| CAREER               | 0.900     | 0.818  | 0.857    | 22.0    |
| HOURS                | 0.906     | 0.983  | 0.943    | 59.0    |
| FLEX_HOURS           | 0.756     | 0.861  | 0.805    | 36.0    |
| SHIFT                | 1.000     | 0.368  | 0.538    | 19.0    |
| LOC                  | 0.846     | 0.208  | 0.333    | 53.0    |
| FLEX_LOC             | 0.842     | 0.800  | 0.821    | 20.0    |
| CONTRACT             | 0.757     | 0.700  | 0.727    | 40.0    |
| LEAVE                | 0.853     | 0.967  | 0.906    | 30.0    |
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

(_Support_ = the number of sentences that were positively labelled for that attribute. For example, we found 52 sentences that contained information about L&D, and 0 sentences that contained information about voice and representation.)

The best performance is achieved on hours, compensation, perks, leave, L&D and career - which all had over 85% F1 score.

The recall is high for the flexible hours job quality measure, but it has a lower precision, indicating a relatively high number of false positives. When we investigate these errors, we find that phrases such as “More shifts will come” and “shifts with early finish” are mistakenly being matched to “choice of shifts”. Additionally, phrases such as “flexibility to be based [in the office or 'hybrid' working from home]” and “working culture and flexible working [(hybrid Woking available)]” are matched to “flexible working”. We can see that these should be in the flexible hours by reading ahead, but the algorithm only got a short part of the text.

When a sentence is labelled as being about location or shifts then it is usually correct, but the algorithm misses a lot of these (low recall). This seems to be the case for locations because the locations of jobs include lots of place names, which aren’t included in our keyword list - such as “Working in Northgate” and “within the Blackpool and surrounding areas”. Often the phrases to do with shifts are classified as flexible hours since “choice of shifts” and “flexible shift patterns” are a flexible hours job quality measure.

The pipeline in its current format could be enhanced in various ways. Those that we would expect to be most useful would be:

- Support for parsing numeric structures. In its current form, the pipeline can identify phrases that are likely to include information related to terms of employment and pay, but the ability to parse this information is not yet included in the package. For example, the pipeline will be able to identify that a sentence like “Regular working hours 9-5 Monday-Friday” contains information about hours, but further processing would be needed to extrapolate how many hours a week this amounts to. We have prototyped some functions to extract such information (see [the code behind the analysis of job quality in the early years sector](https://github.com/nestauk/dap_job_quality/blob/dev/dap_job_quality/analysis/afs_analysis/afs_analysis.ipynb)) but have not yet evaluated the performance of these functions.
- Improve and expand our keyword phrases. For example, by including more phrases we could fix some of the issues distinguishing between flexible hours and shift job quality measures.
- The pipeline is currently insensitive to negatives. For example, it is currently unable to distinguish “Weekend working will be required” from “No weekend working is required” - both will be matched to the “Employment terms” dimension of job quality.

You can read more about the evaluation process [here](https://github.com/nestauk/dap_job_quality/blob/dev/dap_job_quality/analysis/README.md).

## Data

We were only able to label a relatively small number of job adverts due to the time-consuming nature of this process (the training, validation and test sets for the binary classifier comprise [478 job adverts](https://open-jobs-lake.s3.eu-west-1.amazonaws.com/job_quality/sentence_classifier/inputs/labelled/train_val_test_20240725.csv) while the mapping was evaluated against [152 adverts](https://open-jobs-lake.s3.eu-west-1.amazonaws.com/job_quality/outputs/evaluation/evaluation_data_12_08_24_per_sentence_evaluation_28_08_24.csv)). This means that the classifier is trained on a small number of job adverts and evaluated on even fewer, and similarly the end-to-end evaluation is based on fewer than 100 adverts per job quality dimension. These samples will not be representative of the full spectrum of occupations available in the UK labour market so there is a risk that the approach will not generalise well to some of these "unseen" occupations.
