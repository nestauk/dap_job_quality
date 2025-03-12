# Prerequisites

- Access [this xlsx document](https://docs.google.com/spreadsheets/d/1t7r_gHqgTl_VU497U4gQuEElVDl22iY5/edit?gid=931274545#gid=931274545) which contains the full list of 2020 SOC codes. Rows highlighted in yellow indicate SOC codes in scope for this analysis. More info about SOC can be found [here](https://www.ons.gov.uk/methodology/classificationsandstandards/standardoccupationalclassificationsoc). Info on the Nesta SOC classifier that was used to match job titles to SOC codes can be found [here](https://nestauk.github.io/nlp-link/page1/).

- Download the xlsx and save it to `dap_job_quality/analysis/healthcare_analysis/extendedsoc2020structureanddescriptionsexcel161020241.xlsx`.

- Download the ITL1 shapefile from s3: [s3://open-jobs-lake/job_quality/health_social_care/ITL1.geojson](https://eu-west-1.console.aws.amazon.com/s3/object/open-jobs-lake?region=eu-west-1&bucketType=general&prefix=job_quality/health_social_care/ITL1.geojson).

# Pipeline

Step 1:

**`filter_with_soc_codes.py`** This script gets the dataset of job titles that have been mapped to SOC codes, then uses the list of in-scope SOC codes to filter this dataset. It saves both the unique IDs of the job ads that are in scope for this analysis, and the dataframe of job IDs, job titles and SOC codes.

Step 2 (can run in either order):

**`extract_job_quality.py`** This metaflow extracts job quality measures for the jobs in the healthcare sample.

**`extract_salaries.py`** This metaflow gets raw salary info for the jobs in the healthcare sample.

Step 3 (must run after `extract_job_quality.py`):

**`classify_contract_types.py`** This script uses rule-based classification and regexes to determine the contract type of jobs in the sample.

## LLMs for contract classification

I tried using llama 3.2 to classify contract types, rather than relying on rule-based classification. The following notebooks are relevant to this:

- `contract_analysis.ipynb`: this notebook creates a sample balanced by SOC codes and the contract type target phrase (from the taxonomy) that the job ad contains. This gets saved as `contract_evaluation.csv` and was manually uploaded and labelled in [this Google Sheet](https://docs.google.com/spreadsheets/d/1P9AzAu15r6GqinIt6aNnzygL-5S9v6IUiF9C-kFeINk/edit?gid=643293281#gid=643293281).

- `contract_llm.ipynb`: this notebook tries using llama to classify the contract type. This was run on the evaluation sample that was generated in the previous step.

- `evaluate_llama.ipynb`: this notebook takes the results from both the rule-based classification and llama classification and compares them against the human labels. We find that llama performs slightly better (0.83 F1 score as compared to 0.8 for the rule-based classification).

Despite Llama performing slightly better on contract type classification, we pursued rule-based classification for the following reasons:

- It is quicker to implement over a large dataset, so given that the performance between the two methods is similar, opting for the quickest one was the best choice.

- Rule-based classification is deterministic and makes predictable errors. Llama's errors are more difficult to predict and mitigate against. Where human labelling could not determine the contract type, both rule-based classification and Llama had false positives by classifying these as "Permanent" contracts. However, on top of this, Llama also classified a surprising number of sentences where the contract type could not be determined by a human as "Temporary". This seems to be because of incidences where mentions of hours (e.g. "we provide 24/7 cover", "shift rotation", "round the clock care", "term time hours") were interpreted by Llama as being about temporary contracts.
