# Prerequisites

- Access [this xlsx document](https://docs.google.com/spreadsheets/d/1t7r_gHqgTl_VU497U4gQuEElVDl22iY5/edit?gid=931274545#gid=931274545) which contains the full list of 2020 SOC codes. Rows highlighted in yellow indicate SOC codes in scope for this analysis. More info about SOC can be found [here](https://www.ons.gov.uk/methodology/classificationsandstandards/standardoccupationalclassificationsoc). Info on the Nesta SOC classifier that was used to match job titles to SOC codes can be found [here](https://nestauk.github.io/nlp-link/page1/).

- Download the xlsx and save it to `dap_job_quality/analysis/healthcare_analysis/extendedsoc2020structureanddescriptionsexcel161020241.xlsx`.

# Pipeline

Step 1:
**`filter_with_soc_codes.py`** This script gets the dataset of job titles that have been mapped to SOC codes, then uses the list of in-scope SOC codes to filter this dataset. It saves both the unique IDs of the job ads that are in scope for this analysis, and the dataframe of job IDs, job titles and SOC codes.

Step 2 (can run in either order):
**`extract_job_quality.py`** This metaflow extracts job quality measures for the jobs in the healthcare sample.

**`extract_salaries.py`** This metaflow gets raw salary info for the jobs in the healthcare sample.

Step 3 (must run after `extract_job_quality.py`):
**`classify_contract_types.py`** This script uses rule-based classification and regexes to determine the contract type of jobs in the sample.
