from dotenv import load_dotenv
import json
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
import os
import pandas as pd
from pathlib import Path

from dap_job_quality import PROJECT_DIR

GPT_MODEL = "gpt-3.5-turbo"  # "gpt-3.5-turbo-16k"
TEMP = 0.7
SYSTEM_MESSAGE = "You randomly generate sentences from job adverts. You label them 1 if related to job quality, or 0 if not related to job quality."
N_SAMPLES = 10

OUT_FILE = PROJECT_DIR / "inputs/labelled/dummy_job_sentences.jsonl"
OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

load_dotenv()
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

INPUT = """

Generate a job advert sentence and return it with a label if it is related to job quality or not. The label will be 1 if the sentence relates to the compensation package, the contract type, working hours, info about the team you'll be joining or sentences that relate to “why you should want to work here”; it will be 0 if the sentence describes role requirements, "what will be expected of you", "how to apply", or anything else that does not directly determine job quality.

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
            sentence_dicts = []
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
