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

To run the custom prodigy instance (in the `prodigy` directory), run:

```
prodigy benefits_classification job_quality_annotated \
    ./labelled_data/20240117_ads_to_label_ts_1000_random_seed_42.jsonl \
    -F custom_recipe.py
```

To save the outputs of the labelling exercise locally and to s3, run:

```
prodigy db-out job_quality_annotated > dap_job_quality/pipeline/prodigy/labelled_data/20240117_ads_labelled.jsonl
aws s3 cp dap_job_quality/pipeline/prodigy/labelled_data/20240117_ads_labelled.jsonl s3://open-jobs-lake/job_quality/prodigy/labelled_data/20240117_ads_labelled.jsonl
```

### Labelling guidelines

We're trying to see **which dimensions of job quality we can extract from job ads**. We've mapped out the different possible dimensions and would like to assess the feasibility of extracting these dimensions from job ads.

We're interested in the following:

| id  | Dimension           | Sub-element (RSA) | Potential measures    | Feasibility for analysis | Comments |
| --- | ------------------- | ----------------- | --------------------- | ------------------------ | -------- |
| 1   | Terms of employment | Job security      | Job type (perm/ temp) |

<br> | High | |
| Minimum guaranteed hours | Full time/ part time

Working hours (total per week)

Working hours (times of day/ week)

Mentions of casual work/ variable hours | High | |
| Underemployment | N/A | Low | Depends on candidate |
| 2 | <br><br><br><br><br><br><br><br>

Pay and benefits | Pay | Gross salary (range)

Bonus

Pension

Company equity | High | |
| Entitlements | \# Days of leave (annual)

\# Months parental leave

\# Days leave entitlement (other)

Life insurance

Private healthcare

Ride to work

Staff discounts

Travel card scheme

Other

Visa sponsorship

Childcare vouchers | High | Included from additional RSA measure (appendix) |
| Satisfaction with pay | N/A | Low | Depends on candidate |
| 3 | Health, safety and psycho-social wellbeing | Physical injury | N/A | Low | |
| Mental health | EPA

<br> | Medium | Limited to support available |
| 4 | <br><br><br><br><br><br>

Job design and nature of work | Use of skills

<br> | N/A | Low | Depends on candidate |
| Control/ autonomy | Free text on these themes

[Skills mapping?] | Medium | Are there skills more commensurate with this? |
| Opportunities for progression | Free text on these themes | Medium | |
| Sense of purpose | | Medium | Is there an industry aspect here? |
| Learning and development | Free text of this theme | Medium | Suggest adding to RSA as specific category |
| Recognition [non-RSA] | Free text on the theme of recognition, appreciation | Medium | |
| 5 | Social support and cohesion | Peer support | N/A | Low | Not meaningfully advertised |
| Line manager relationship | N/A | Low | Not meaningfully advertised |
| Atmosphere, culture, environment [non-RSA] | Free text relating to team / company culture | Medium | |
| 6 | <br><br><br>

Voice and representation | Trade union membership | N/A | Low | Can be inferred from some extent by industry/ occupation |
| Employee information | N/A | Low | |
| Employee Involvement | Employee support networks

Disability support networks | Medium | |
| 7 | <br><br><br><br>

Work-life balance | Over-employment | Advertisements of long hours, high pressure, or short deadlines | Medium | Somewhat subjective |
| Overtime | Hourly rates for overtime | High | |
| Flexibility | Flexible work hours (eg, compressed hours)

Flexible location (inc. remote/ hybrid work) | High | |
