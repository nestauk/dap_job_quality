# Job Quality Extractor

## Overview

This package is designed to extract and analyze job quality measures from job adverts using natural language processing techniques. The package identifies job quality aspects based on sentence similarity and pre-defined target phrases, helping to classify and quantify job quality indicators from large datasets of job descriptions. This work was funded by the [Economic Statistics Centre of Excellence](https://www.escoe.ac.uk/).

## What dimensions of job quality do you extract?

The term "job quality" refers to aspects of a job that affect worker wellbeing - for example how much the job is paid, and whether the contract is permanent. Most research on job quality rightly focuses on data from the employee's point of view, using surveys or interviews or, recently, [online reviews](https://www.escoe.ac.uk/publications/extracting-dimensions-of-job-quality-from-online-employee-reviews/).

We took as our starting point [CIPD's seven dimensions of job quality](https://www.cipd.org/globalassets/media/knowledge/knowledge-hub/reports/2023-pdfs/2023-good-work-index-report-8407.pdf):

1. pay and benefits
2. contract (elsewhere called terms of employment)
3. work-life balance
4. job design and the nature of work
5. relationships at work
6. employee voice
7. health and wellbeing

We also added an additional category, ‘barriers to access’, to our taxonomy, so that dimensions of job quality that directly impact marginalised groups might be gathered together. We made one further addition, “atmosphere, culture and environment”, which fits under “Social support and cohesion” and which we took from Sleeman 2024.
Our taxonomy of job quality can be seen [here](https://docs.google.com/spreadsheets/d/1ORiJDeNWvhcEMSG6i-rfatzpbJzFq0uo6XHbny6xxsE/edit?gid=0#gid=0).
## Installation

To install the package, run

```
pip install git+https://github.com/nestauk/dap_job_quality.git
```

## Quickstart

To extract dimensions of job quality from a single job advert or from a list of job adverts, you can use the `extract_job_quality()` function. This function takes a dataframe of job adverts as input, and returns

- A dataframe with the job adverts split into sentences; each sentence is labelled 0 or 1 according to whether it is related to job quality, and sentences labelled 1 are also matched to the taxonomy.
- A concise dict which just contains the ID of each advert, and the target phrases that it was matched to.

Example usage:

```python
from dap_job_quality.pipeline.find_job_quality import JobQuality
import pandas as pd

# Initialize JobQuality class
job_quality = JobQuality()
job_quality.load()

# Example job adverts dataframe
job_adverts = pd.DataFrame(
    [
        {'id': 123, 'description': '[This is a job advert. It has many benefits such as a pension scheme and a cycle to work scheme.]'},
        {'id': 234, 'description': '[This is a job advert for a bank job. There are free childcare vouchers. We also offer a yearly bonus and generous salary.]'}
    ]
)

# Extract job quality
jq_df_filtered, job_id_to_target_phrase = job_quality.extract_job_quality(
    job_adverts, id_col="id", text_col="description"
)
```

The output dataframe `jq_df_filtered` should look like this:
| id | description | clean_description | job_quality_label | sentences_split | ngrams | target_phrase | cosine_similarity | subcategory |
|-----|-------|-----------|----------|-------|--------|--------|---------|----------|
| 123 | [This is a job advert. It has many benefits su... | This is a job advert. It has many benefits suc... | LABEL_1 | It has many benefits such as a pension scheme ... | a cycle to work | Cycle to work | 0.965111 | PERKS |
| 123 | [This is a job advert. It has many benefits su... | This is a job advert. It has many benefits suc... | LABEL_1 | It has many benefits such as a pension scheme ... | many benefits such as | benefits | 0.874949 | PERKS |
| 123 | [This is a job advert. It has many benefits su... | This is a job advert. It has many benefits suc... | LABEL_1 | It has many benefits such as a pension scheme ... | such as a pension | pension | 0.821573 | COMP |
| 123 | [This is a job advert. It has many benefits su... | This is a job advert. It has many benefits suc... | LABEL_1 | It has many benefits such as a pension scheme ... | a pension scheme and | pension scheme | 0.964935 | COMP |
| 234 | [This is a job advert for a bank job. There ar... | This is a job advert for a bank job. There are... | LABEL_1 | There are free childcare vouchers. | There are free childcare vouchers. | childcare vouchers | 0.838904 | CARING |
| 234 | [This is a job advert for a bank job. There ar... | This is a job advert for a bank job. There are... | LABEL_1 | We also offer a yearly bonus and generous salary. | bonus and generous salary. | compensation | 0.576268 | COMP |
| 234 | [This is a job advert for a bank job. There ar... | This is a job advert for a bank job. There are... | LABEL_1 | We also offer a yearly bonus and generous salary. | a yearly bonus and | performance bonus | 0.618560 | COMP |

Meanwhile, the more concise output, `job_id_to_target_phrase`, should look like this:

```python
{
    123: ['Cycle to work', 'benefits', 'pension', 'pension scheme'],
    234: ['childcare vouchers', 'compensation', 'performance bonus']
 }
```

## How does it work?

The pipeline comprises 4 basic steps:

1. Clean the text minimally, then separate the advert into sentences
2. Classify the sentences as either relating to job quality (eg "We are a friendly supportive team") or not relating to job quality (eg "You must have a friendly supportive demeanour"). More detail on the classifier [here](./classifier.md) and in the [README](./pipeline/sentence_classifier/README.md).
3. Chunk up the sentences
4. Match the sentence chunks to the taxonomy (more detail on steps 3 and 4 [here](./taxonomy_mapping.md))
