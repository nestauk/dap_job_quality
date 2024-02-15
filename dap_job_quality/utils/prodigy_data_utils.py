"""
Helper functions for handling data that has been labelled in Prodigy.
"""

from spacy.tokens import Span, Doc
import spacy
import srsly
from typing import List, Dict, Any

nlp = spacy.load("en_core_web_sm")


def read_accepted_lines(file: str) -> List[Dict[str, Any]]:
    """
    Reads in data from a Prodigy-annotated file and returns only the accepted records.

    Args:
        file (str): The file path to read from.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each representing an accepted record.
    """
    records = []

    for line in srsly.read_jsonl(file):
        if line["answer"] == "accept":
            records.append(line)

    return records


def get_spans_and_sentences(
    records: List[Dict[str, Any]]
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Processes annotated records to extract labelled spans of text, their labels and the whole sentences in which they occur.

    Args:
        records (List[Dict[str, Any]]): A list of dictionaries, each representing an accepted record from Prodigy.

    Returns:
        Dict[str, List[Dict[str, Any]]]: A dictionary where each key is a 'job_id' and the value is a list of dictionaries.
                                         Each dictionary in the list contains information about a span, including the text,
                                         the whole sentence it belongs to, the label, and the full text of the record.
    """
    training_data = {}

    for record in records:
        # convert each text to a spacy document
        doc: Doc = nlp(record["text"])
        # get the labelled spans within each document
        spans = record["spans"]
        spans_parsed = []
        if len(spans) > 0:
            # map the span back to the text it corresponds to
            for span in spans:
                span_data = {}
                current_span = Span(
                    doc,
                    span["token_start"],
                    span["token_end"] + 1,
                    span["label"],
                )
                span_data["span"] = current_span.text
                span_data["sent"] = current_span.sent
                span_data["label"] = span["label"]
                span_data["text"] = record["text"]
                spans_parsed.append(span_data)
        else:
            spans_parsed.append(
                {"span": "", "sent": "", "label": "none", "text": record["text"]}
            )
        training_data[record["meta"]["job_id"]] = spans_parsed

    return training_data
