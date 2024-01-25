from spacy.tokens import Span
import spacy
import srsly

nlp = spacy.load("en_core_web_sm")


def read_accepted_lines(file):
    """Read in Prodigy data and return only the accepted texts"""
    records = []

    for line in srsly.read_jsonl(file):
        if line["answer"] == "accept":
            records.append(line)

    return records


def get_spans_and_sentences(records):
    """Get the labelled spans with their labels, and the sentences in which they occur, from prodigy data"""
    training_data = {}

    for record in records:
        # convert each text to a spacy document
        doc = nlp(record["text"])
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
