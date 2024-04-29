
# Sentence classifier pipeline

## Set up
* If you want to run the modelling pipeline, make sure your `.env` file contains a variable called `WANDB_ENTITY`
* To run `generate_dummy_data.py`, you will need to add your OpenAI API key to `.env` as `OPENAI_API_KEY`

## How to use

The scripts run in this order:

`prep_training_data.py`: takes a labelled sample of job ads (labelled by Clare) and turns this into both positive and negative examples. This gets split into training, validation and test sets.

`baseline_classifier.py`: this runs a dummy classifier (based on probabilities calculated from the training set) in order to get some baseline metrics.

`log_reg.py`: this embeds the sentences using [jobbert-base-uncased](https://huggingface.co/jjzha/jobbert-base-cased) and trains a logistic regression model. The confusion matrix, false positives and false negatives get logged on weights and biases.

## Other scripts
`generate_dummy_data.py` - this script generates some made-up job advert sentences using langchain. It uses a few-shot prompting approach. The resulting data is not currently being used, but can be retrieved with the getter `get_dummy_job_sentences()` in `dap_job_quality/getters/labelled_data.py`.
