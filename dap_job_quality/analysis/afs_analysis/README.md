
The notebooks in this folder contain the analysis of job quality in adverts for Early Years Practitioner positions and comparable occupations.

# Sample creation

A sample of job adverts from relevant sectors is initially created as part of a separate project [here](https://github.com/nestauk/afs_early_years_labour_market_analysis). **Note that the job titles and sector names follows those native to the job site these adverts were taken from.**

The sample for the current analysis is then created by the following scripts:
- `dap_job_quality/pipeline/stratified_sample.py` (creates a stratified sample)
- `dap_job_quality/pipeline/eyp_auxiliary_data.py` (extracts salary information for adverts in the stratified sample)

To ensure balance across sectors and regions, ITL1 regions are grouped together as follows:
- "North": North East (England); North West (England); Yorkshire and the Humber
- "Midlands": West Midlands (England); East Midlands (England)
- "South": South East (England); South West (England); East of England
- London

Similarly, among the professions, jobs listed under "Primary School Teacher" and "Secondary School Teacher" are grouped together, and "Retail Assistant" and "Waiter" are grouped together. Large enough samples were present for "Early Years Practitioner", "Teaching Assistant", "Supply Teacher" and "Special Needs Teacher", so no grouping of those was required.

Only adverts from England were included.

# Calculation of hourly pay

The job site gives salary in units of either hour, day or year. We used the following logic to estimate hourly pay:
- If the unit given is hour, use this as the final value for hourly pay
- If the unit given is day, divide this by 7.5 (assume that a standard working day is 7.5 hours)
- If the unit given is year, divide by (37.5*52=1950). (Assume that a standard working week is 37.5 hours and that if the unit given is year, the salary is expressed as a per annum value which would be pro rata for a part-time role).

We used functions that can be found in `dap_job_quality/utils/analysis_utils.py` to apply this logic to the dataset.

# Contract type

We have also tried to estimate contract type for as many of the adverts as possible. Our processing followed these steps:
- Identify sentences that are related to contract, using the output of the job quality pipeline.
- Using sets of keywords related to permanent contracts, temporary contracts and apprenticeships, use regex to classify these sentences as pertaining to one contract type. These keywords were initially chosen and then augmented by examining the most frequently occurring words in sentences that had been identified as being about contract.
- Given that a job advert contains multiple sentences, and multiple sentences within it may mention contract: if more than one contract type is matched to the job advert, take as final whichever contract type has more matches within that advert.

