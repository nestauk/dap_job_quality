import pytest
from dap_job_quality.utils.text_cleaning import (
    clean_text,
)  # Import the clean_text function from where it is defined.


def test_clean_text_with_camelcase():
    text = "This is an exampleText that needsProcessing."
    expected_result = "This is an example. Text that needs. Processing."
    assert clean_text(text) == expected_result, "Failed to handle camelCase text."


def test_clean_text_sentences():
    text = "Double digit growth.Progression opportunities.10K commission + benefits"
    expected_result = (
        "Double digit growth. Progression opportunities. 10K commission + benefits"
    )
    assert clean_text(text) == expected_result, "Failed to add spaces after full stops."


def test_clean_text_with_no_changes():
    text = "This is a simple sentence."
    expected_result = "This is a simple sentence."
    assert (
        clean_text(text) == expected_result
    ), "Failed with text that requires no changes."


def test_clean_text_preserve_monetary():
    text = "£14.77 per hour"
    expected_result = "£14.77 per hour"
    assert clean_text(text) == expected_result, "Failed to preserve pay information."


def test_clean_text_preserve_exceptions():
    text = "Skills required: JavaScript"
    expected_result = "Skills required  JavaScript"  # colon gets replaced by a space, so now there are 2 spaces
    assert (
        clean_text(text) == expected_result
    ), "Failed to preserve camelcase exceptions."


# def test_clean_text_with_compound_case():
#     text = "Complex case: startingNewProject now, right?"
#     expected_result = ["Complex case:", "Starting. New. Project now,", "right?"]
#     assert clean_text(text) == expected_result, "Failed with complex mixed cases."
