# Identifying job quality-related content in job adverts

We trained a binary classifier to distinguish sentences that contain information relating to job quality from other types of sentence in job adverts (e.g. company description, or requirements for the role). For example, in terms of job quality, "You must be willing to work flexible shifts" is different from "We offer flexible working hours", and similarly "We are a friendly supportive team" is different from "You must have a friendly supportive demeanour".

The final model is a fine-tuned instance of [jobbert-base-cased](https://huggingface.co/jjzha/jobbert-base-cased). We opted to fine-tune this model for the following reasons:

- when compared with a fine-tuned Distilbert model and a logistic regression, the jobbert model performed marginally better than Distilbert in terms of F1 score
- the jobbert model is optimised for use on job advert text
- this model had already been fine-tuned successfully for our [company description classifier](https://github.com/nestauk/ojd_daps_company_descriptions)

For more detail on how this model was trained see [this readme](https://github.com/nestauk/dap_job_quality/blob/dev/dap_job_quality/pipeline/sentence_classifier/README.md).
