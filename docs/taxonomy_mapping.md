# Matching text to the job quality taxonomy

## Taxonomy development

We conducted an initial analysis to assess (a) which, if any, dimensions of job quality might manifest in job adverts, and (b) what language would be used to express these. We then created a taxonomy that comprised:

- The higher level dimension of job quality, eg “Job design and nature of work”
- Sub-categories within that which were taken from the [CIPD Good Work Index 2023](https://www.cipd.org/globalassets/media/knowledge/knowledge-hub/reports/2023-pdfs/2023-good-work-index-report-8407.pdf) and [Measuring Good Work](https://d1ssu070pg2v9i.cloudfront.net/pex/pex_carnegie2021/2018/09/06105222/Measuring-Good-Work-FINAL-03-09-18.pdf), eg “career progression”, “learning and development”, “sense of purpose”
- The most common phrases that we saw in job adverts that related to these sub-categories. For example, for the sub-category “learning and development”, we included the phrases “CPD” (Continuous Professional Development), “learning and development”, “training”.

This means that not all of the CIPD's dimensions and subdimensions of job quality are in our taxonomy, because some do not appear or cannot be inferred from job adverts. For example, trade union existence and activity is not something that is typically mentioned in job adverts. Similarly, the subdimension "use of skills" refers to whether an employee is in employment that makes use of their specific skillset, so it is dependent on the individual and cannot be inferred from the job advert.

You can find the final taxonomy [here](https://open-jobs-lake.s3.eu-west-1.amazonaws.com/job_quality/keywords/keyword_lookup+-+v8.csv). In summary, the dimensions we seek to capture are:

- CAREER: falling under _Job design and nature of work_, this category is about opportunities for career progression and advancement.

- COMP: falling under _Pay and benefits_, this category is about the salary and other financial benefits such as pension, bonus etc.

- CONTRACT: falling under _Terms of employment_, this category is about the type of contract offered, eg permanent, temporary, fixed-term, zero-hours.

- FLEX*HOURS: falling under \_Work-life balance*, this includes any references to flexible hours, job-share, and other flexible working arrangements. Note that "part-time" is included separately in the HOURS category.

- FLEX*LOC: falling under \_Work-life balance*, this includes any references to remote working, hybrid working, or other flexible location arrangements.

- HEALTH: falling under _Health, safety and psycho-social wellbeing_, we included any references to sick pay or private healthcare under this category.

- HOURS: falling under _Terms of employment_, this category is intended to pick up any reference to the number of contracted hours.

- L&D: falling under _Job design and nature of work_, this category is about learning and development opportunities, such as training, CPD, or qualifications.

- LEAVE: falling under _Pay and benefits_, this category is about annual leave. We have included sick leave and parental leave under other categories.

- LOC: falling under _Terms of employment_, this category is about the location of the job, eg city, region, or country.

- PERKS: falling under _Pay and benefits_, this category is about non-financial benefits, such as cycle to work schemes, gym memberships, shopping vouchers and so on.

- SHIFT: falling under _Terms of employment_, this category is about shift patterns, eg night shifts, weekend shifts, or rotating shifts.

Categories that we attempted to extract but that we do not recommend for others to use are (see further [our evaluation of the pipeline](https://github.com/nestauk/dap_job_quality/tree/dev/dap_job_quality/analysis)):

- AUTONOMY: falling under _Job design and nature of work_, this category is about the extent to which employees have control over their work. Only 1 example was found in the evaluation sample.

- CARING: falling under the dimension we have called _Barriers to access_, this category is intended to capture anything related to caring duties, such as parental leave, carers' leave, or whether the role is term-time only (but note there are separate flexible working categories as well, FLEX_HOURS and FLEX_LOC). Only 8 positive examples were found in the evaluation sample.

- DISABILITY: falling under _Barriers to access_, this category was intended to be about whether the job is suitable for people with disabilities. In practice, we found that many job adverts contain boilerplate text about being an equal opportunities employer; we labelled such text as NOT relating to disability on the rationale that absence of discrimination is not the same as positive behaviour in terms of hiring disabled candidates. In contrast, we labelled text that indicates that an employer is disability positive as being related to this category. Despite our attempts to label text in this way, there is still a risk of false positives for this category. Only 2 positive examples were found in the evaluation sample.

- M*HEALTH: falling under \_Health, safety and psycho-social wellbeing*, this category is about mental health support. Only 4 positive examples were found in the evaluation sample.

- MISC: this was intended as a "catch-all" category and in practice contains a mix of different types of information. Only 13 positive examples were found in the evaluation sample.

- REWARD: falling under _Job design and nature of work_, this category is about non-monetary reward. Only 3 positive examples were found in the evaluation sample. In practice we expect this category to produce false positives because of the common use of the word "reward" in job adverts to mean salary and financial compensation.

- SOCIAL: this falls under the dimension _Social support and cohesion_. The performance of this category was poor in the evaluation. Only 22 positive examples were found in the evaluation sample.

- SPONSORSHIP: this refers to offers of visa sponsorship and falls under _Pay and benefits_. Only 2 positive examples were found in the evaluation sample. We expect this category to be difficult to extract without further parsing because of the prevalence of near-matches such as "We cannot offer visa sponsorship" in job advert text.

- VOICE REPRESENTATION. No positive examples were found in the evaluation sample.

With more data, performance on these latter categories could potentially be improved. See further [here](./limitations.md).

## Using a different taxonomy

We developed a taxonomy that fit our purposes, but it is possible to use your own taxonomy - though we cannot guarantee performance, as our pipeline has been evaluated against the specific taxonomy we developed. If you wish to use your own taxonomy, the steps that you will need to follow are:

1. Modify the getter function [get_keywords()](https://github.com/nestauk/dap_job_quality/blob/dev/dap_job_quality/getters/keywords.py) so that it points to your taxonomy file. This should be a dataframe (saved as `.csv`, `.parquet` or similar) with the columns `dimension`, `sub_category`, `target_phrase`. `target_phrase` contains the strings that will be embedded and compared to the job advert text.
2. You should evaluate the performance of the pipeline against your taxonomy. This can be done using the scripts in [`dap_job_quality/analysis/mapping_evaluation/`](https://github.com/nestauk/dap_job_quality/blob/dev/dap_job_quality/analysis/README.md) and the notebook [`dap_job_quality/notebooks/Evalution.ipynb`](https://github.com/nestauk/dap_job_quality/blob/dev/dap_job_quality/notebooks/Evaluation.ipynb).

## Mapping approach

The overall approach taken is to chunk up an input sentence into pieces that are then compared to target phrases from a taxonomy.

1. **Initial cleaning**: The input sentence is chunked up into smaller pieces. At first, it is split up on characters that commonly indicate a list, eg ":" (see [`split_text()`](https://github.com/nestauk/dap_job_quality/blob/647565433a4ce21e510de0b574443d516fc5a037/dap_job_quality/pipeline/find_job_quality.py#L91)). Digits are also replaced with 'X' because for our purposes, the exact number is not important: for example, we would like our pipeline to treat "25 days of annual leave" and "30 days of annual leave" as the same.

2. **Sentence chunking**: Then, if the sentence is 6 words or fewer, it is kept whole; otherwise, a rolling window of 4 words is applied. We will refer to these smaller chunks as **ngrams** - mostly they will be 4 words long, but some may be shorter and some may be as long as 6 words. We chose this rolling window size as it is similar to the lengths of the target phrases we match to in the next steps.

3. **Embedding**: both the ngrams and the target phrases from the taxonomy are embedded, both using the pretrained model [`all-MiniLM-L6-v2`](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2). This model was chosen because it is relatively small, optimised for sentence similarity tasks, and has been pretrained on a large corpus of text.

4. **Cosine similarity**: The cosine similarity between the ngrams and the target phrases is calculated. If the similarity is above a certain threshold, the ngram is considered a match to the target phrase.

Different subcategories within the taxonomy have different thresholds:

| Category   | Cosine similarity threshold |
| ---------- | --------------------------- |
| CAREER     | 0.6                         |
| FLEX_HOURS | 0.65                        |
| HOURS      | 0.6                         |
| FLEX_LOC   | 0.6                         |
| LEAVE      | 0.65                        |
| CONTRACT   | 0.5                         |
| LOC        | 0.6                         |
| OTHER      | 0.55                        |

These are intended to optimise for precision vs. recall metrics for each subcategory. See the notebook [`Evaluation.ipynb`](https://github.com/nestauk/dap_job_quality/blob/dev/dap_job_quality/notebooks/Evaluation.ipynb) for plots of precision vs. recall trade-offs at different thresholds for different categories. This notebook also demonstrates that 0.55 is the most reasonable threshold value for categories other than "CAREER", "FLEX_HOURS", "HOURS", "FLEX_LOC", "LEAVE", "CONTRACT" and "LOC", as it is the median of the thresholds for other categories (we focused on "CAREER", "FLEX_HOURS", "HOURS", "FLEX_LOC", "LEAVE", "CONTRACT" and "LOC" for the evaluation because they were prioritised for downstream analysis).

5. **Finding the best match**: Often, multiple ngrams within a sentence will be matched to the same subcategory. In this case, the ngram with the highest cosine similarity is chosen as the best match. This is demonstrated in the figure below:

![Mapping approach](img/ngram_mapping.png)
