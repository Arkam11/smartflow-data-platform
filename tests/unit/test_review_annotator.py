import pytest
import json
from unittest.mock import patch, MagicMock
from src.annotation.review_annotator import (
    build_annotation_prompt,
    call_openai_api,
    save_annotations,
)


def test_build_prompt_includes_review_ids():
    reviews = [
        {"review_id": "rev_001", "rating": 5,
         "review_text": "Great product!"},
        {"review_id": "rev_002", "rating": 1,
         "review_text": "Terrible experience"},
    ]
    prompt = build_annotation_prompt(reviews)
    assert "rev_001" in prompt
    assert "rev_002" in prompt


def test_build_prompt_requests_json_only():
    reviews = [{"review_id": "rev_001", "rating": 4,
                "review_text": "Good delivery"}]
    prompt = build_annotation_prompt(reviews)
    assert "JSON" in prompt
    assert "sentiment" in prompt
    assert "confidence" in prompt


def test_call_openai_api_parses_direct_array():
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.choices[0].message.content = json.dumps([{
        "review_id": "rev_001",
        "sentiment": "positive",
        "confidence": 0.95,
        "primary_topic": "delivery",
        "needs_human_review": False
    }])
    mock_client.chat.completions.create.return_value = mock_response

    result = call_openai_api("test prompt", mock_client)

    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["sentiment"] == "positive"


def test_call_openai_api_parses_wrapped_object():
    """OpenAI json_object mode sometimes wraps arrays in a dict."""
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.choices[0].message.content = json.dumps({
        "annotations": [{
            "review_id": "rev_002",
            "sentiment": "negative",
            "confidence": 0.88,
            "primary_topic": "product_quality",
            "needs_human_review": False
        }]
    })
    mock_client.chat.completions.create.return_value = mock_response

    result = call_openai_api("test prompt", mock_client)

    assert isinstance(result, list)
    assert result[0]["sentiment"] == "negative"


def test_save_annotations_handles_empty_list():
    mock_engine = MagicMock()
    saved = save_annotations([], mock_engine)
    assert saved == 0


def test_annotation_sentiment_values_are_valid():
    valid_sentiments = {"positive", "neutral", "negative"}
    sample_annotations = [
        {"review_id": "r1", "sentiment": "positive",
         "confidence": 0.9, "primary_topic": "delivery",
         "needs_human_review": False},
        {"review_id": "r2", "sentiment": "negative",
         "confidence": 0.85, "primary_topic": "product_quality",
         "needs_human_review": False},
    ]
    for ann in sample_annotations:
        assert ann["sentiment"] in valid_sentiments
        assert 0.0 <= ann["confidence"] <= 1.0