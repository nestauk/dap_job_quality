import boto3
from botocore.exceptions import NoCredentialsError
from datetime import date
from dotenv import load_dotenv
import json
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
import os

# import pandas as pd
from pathlib import Path

from dap_job_quality import PROJECT_DIR, BUCKET_NAME

GPT_MODEL = "gpt-3.5-turbo"  # "gpt-3.5-turbo-16k"
TEMP = 0.7
SYSTEM_MESSAGE = "You randomly generate sentences from job adverts. You label them 1 if related to job quality, or 0 if not related to job quality."
N_SAMPLES = 100

FILENAME = f"dummy_job_sentences_{date.today()}.jsonl"
S3_PATH = f"job_quality/sentence_classifier/inputs/labelled/{FILENAME}"
OUT_FILE = PROJECT_DIR / f"inputs/labelled/{FILENAME}"
OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

load_dotenv()
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

INPUT = """

Generate a job advert sentence and return it with a label if it is related to job quality or not. The label will be 1 if the sentence relates to the compensation package, the contract type, working hours, info about the team you'll be joining or sentences that relate to “why you should want to work here”; it will be 0 if the sentence describes role requirements, "what will be expected of you", "how to apply", or anything else that does not directly determine job quality.

Avoid generic statements like "We offer a competitive salary and benefits package". Instead, focus on specific details that would be found in a job advert.

Examples:
sentence: This job provides ample opportunities for career development and progression.
label: 1

sentence: Salary: £41,000 - £42,000.
label: 1

sentence: variable shift patterns of: Early- 7.00am – 16.00; Late- 12.00 noon
label: 1

sentence: You will be joining a small, supportive, close-knit team.
label: 1

sentence: We are an industry leading brewer of craft beers.
label: 0

sentence: Are you an organised self-starter, who can multi-task and prioritise work?
label: 0

sentence: You'll be acting as technical and cultural change catalysts, accelerating learning journeys and the progressive adoption of the DevOps centre of excellence.
label: 0

sentence: A degree in Civil Engineering or Engineering Geology, with a 2:1 classification (or equivalent) or above.
label: 0

sentence: This role requires strong attention to detail and the ability to meet deadlines consistently.
label: 0
"""


def upload_file_to_s3(local_file, bucket_name, s3_file_name):
    """
    Upload a file to an S3 bucket

    :param local_file: File to upload
    :param bucket_name: Bucket to upload to
    :param s3_file_name: S3 object name. If not specified then local_file is used
    """
    # Create an S3 client
    s3 = boto3.client("s3")

    try:
        # Upload the file
        s3.upload_file(local_file, bucket_name, s3_file_name)
        print(f"File {local_file} uploaded to {bucket_name}/{s3_file_name}")
    except FileNotFoundError:
        print(f"The file {local_file} was not found")
    except NoCredentialsError:
        print("Credentials not available")


if __name__ == "__main__":
    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                SYSTEM_MESSAGE,
            ),
            ("user", "{input}"),
        ]
    )

    answers = []

    llm = ChatOpenAI(
        openai_api_key=OPENAI_API_KEY, model_name=GPT_MODEL, temperature=TEMP
    )

    output_parser = StrOutputParser()

    chain = prompt | llm | output_parser

    for i in range(N_SAMPLES):
        answers.append(chain.invoke({"input": INPUT}))

    # all_sentences =[]

    with open(OUT_FILE, "w") as file:
        for answer in answers:
            blocks = answer.strip().split("\n\n")
            # sentence_dicts = []
            for block in blocks:
                lines = block.split("\n")

                first_line_split = lines[0].split(": ")
                if len(first_line_split) == 1:
                    first_line_split[0]
                else:
                    sentence = first_line_split[1]

                if len(lines) > 1:
                    second_line_split = lines[1].split(": ")
                    if len(second_line_split) == 1:
                        label = int(second_line_split[0])
                    else:
                        label = int(second_line_split[1])
                else:
                    label = -1

                sent_dict = {"sentence": sentence, "label": label}
                sent_dict_string = json.dumps(sent_dict)
                file.write(sent_dict_string + "\n")

    upload_file_to_s3(OUT_FILE, BUCKET_NAME, S3_PATH)
