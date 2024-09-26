# Identifying job quality-related content in job adverts

We trained a binary classifier to distinguish sentences that contain information relating to job quality from other types of sentence in job adverts (e.g. company description, or requirements for the role). For example, in terms of job quality, "You must be willing to work flexible shifts" is different from "We offer flexible working hours", and similarly "We are a friendly supportive team" is different from "You must have a friendly supportive demeanour".

The final model is a fine-tuned instance of [jobbert-base-cased](https://huggingface.co/jjzha/jobbert-base-cased). We opted to fine-tune this model for the following reasons:

- when compared with a fine-tuned Distilbert model and a logistic regression, the jobbert model performed marginally better than Distilbert in terms of F1 score
- the jobbert model is optimised for use on job advert text
- this model had already been fine-tuned successfully for our [company description classifier](https://github.com/nestauk/ojd_daps_company_descriptions)

For more detail on how this model was trained see [this readme](https://github.com/nestauk/dap_job_quality/blob/dev/dap_job_quality/pipeline/sentence_classifier/README.md).

## Fine-tuning jobbert

If you wish to fine-tune jobbert on your own data, rather than using the version we have fine-tuned, you will need to take the following steps:

1. Create labelled job adverts. These should be split into sentences, the sentences labelled 0 or 1 according to whether they relate to job quality, and stored in dataframes with the columns 'id' (the unique identifier for the job advert), 'sentence', 'label'.
2. Modify the `get_df()` function in `dap_job_quality/dap_job_quality/getters/train_val_test.py` to point to your training, validation and test dataframes, which should be in the format described above.
3. Set up a [weights and biases](https://wandb.ai/site) account to allow you to run model sweeps.
4. Run the script `jobbert_sweep.py`. This will conduct a hyperparameter sweep over the hyperparameter variations you specify in `dap_job_quality/config/jobbert_config.yaml`. The best hyperparameters from the sweep will be saved using the `get_best_hyperparams()` function from `dap_job_quality/pipeline/sentence_classifier/classifier_utils.py`. This function by default saved the hyperparameters both locally and to an S3 bucket.
5. You should then update `dap_job_quality/config/jobbert_config.yaml`: update the values under `train_config` to reflect the best hyperparameters found in the sweep. You can also modify the number of training epochs you wish to train for and the early stopping patience if desired.
6. Now, you can run `dap_job_quality/pipeline/sentence_classifier/jobbert_train.py`.

To use the model that you have trained for making predictions, update the function `get_jobbert_jq()` in `dap_job_quality/dap_job_quality/getters/jobbert_jq.py` to point to the model that you have trained.

You can use it to make predictions like so:

```python
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline

# Load the tokenizer and model from the specific checkpoint
checkpoint_path = "path/to/best/model"  # Replace with the local path where you have downloaded the model or where the best model was saved at the end of training
tokenizer = AutoTokenizer.from_pretrained(checkpoint_path)
model = AutoModelForSequenceClassification.from_pretrained(checkpoint_path)

model_pipeline = pipeline('text-classification', model=model, tokenizer=tokenizer)

model_pipeline(["You will get 25 days annual leave",
                "Permanent contracts are available",
                "Salary £30,000 per annum",
                "You must be proficient in Python",
                "We are Nesta, the UK’s innovation agency for social good."])
```

Expected output would be something like:

```
[{'label': 'LABEL_1', 'score': 0.9085968136787415},
 {'label': 'LABEL_1', 'score': 0.8105072975158691},
 {'label': 'LABEL_1', 'score': 0.8970603942871094},
 {'label': 'LABEL_0', 'score': 0.8406273126602173},
 {'label': 'LABEL_0', 'score': 0.7921993732452393}]
```
