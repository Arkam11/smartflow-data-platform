import os
import time
import json
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

logging.basicConfig(
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 10
MAX_REVIEWS = 500
BATCH_DELAY = 1.0


def get_engine():
    db_url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}"
        f"/{os.getenv('DB_NAME')}"
    )
    return create_engine(db_url)


def load_reviews_to_annotate(engine, limit: int = MAX_REVIEWS) -> pd.DataFrame:
    """
    Load reviews that have text but no sentiment label yet.
    Runs safely multiple times — only picks up unannotated reviews.
    """
    query = f"""
        SELECT review_id, order_id, rating, review_text
        FROM silver.reviews
        WHERE review_text IS NOT NULL
          AND TRIM(review_text) != ''
          AND sentiment_label IS NULL
        LIMIT {limit}
    """
    df = pd.read_sql(query, engine)
    logger.info(f"Loaded {len(df):,} reviews to annotate")
    return df


def build_annotation_prompt(reviews_batch: list) -> str:
    """
    Build the prompt asking the LLM to annotate a batch of reviews.

    Key prompt engineering principles:
    1. Clear role — "you are a data annotation system"
    2. Exact output format specified — JSON array only
    3. All valid values listed explicitly — no ambiguity
    4. Language note — reviews are in Portuguese
    5. Rating as a signal — guides the model
    """
    reviews_text = ""
    for i, review in enumerate(reviews_batch):
        reviews_text += (
            f"\nReview {i+1}:\n"
            f"  review_id: {review['review_id']}\n"
            f"  rating: {review['rating']}\n"
            f"  text: {review['review_text']}\n"
        )

    prompt = f"""You are a data annotation system for an e-commerce platform.
Analyze each customer review and return ONLY a JSON array with no preamble,
no explanation, and no markdown code blocks.

For each review return an object with exactly these fields:
- review_id: the exact review_id provided
- sentiment: one of exactly ["positive", "neutral", "negative"]
- confidence: a float between 0.0 and 1.0 indicating your confidence
- primary_topic: one of exactly ["delivery", "product_quality", "pricing",
  "customer_service", "packaging", "other"]
- needs_human_review: true if the text is ambiguous, offensive, or unclear

Reviews may be in Portuguese — analyze sentiment from meaning, not language.
Use the star rating as a signal: 4-5 stars lean positive, 1-2 stars lean
negative, 3 stars lean neutral — but text content takes priority.

Reviews to annotate:
{reviews_text}

Return ONLY the JSON array, nothing else. Example format:
[{{"review_id": "abc123", "sentiment": "positive", "confidence": 0.95,
"primary_topic": "delivery", "needs_human_review": false}}]"""

    return prompt


def call_openai_api(prompt: str, client: OpenAI) -> list:
    """
    Call the OpenAI API and parse the JSON response.

    We use gpt-4o-mini — fast, cheap, and more than capable
    for annotation tasks. response_format forces JSON output,
    which eliminates markdown fence stripping entirely.
    """
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": "You are a data annotation system. Always respond with valid JSON only."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        response_format={"type": "json_object"},
        temperature=0.1,  # low temperature = more consistent, less creative
        max_tokens=1024
    )

    response_text = response.choices[0].message.content.strip()

    # OpenAI's json_object mode wraps arrays in an object
    # It may return {"annotations": [...]} or {"reviews": [...]} or just [...]
    parsed = json.loads(response_text)

    # Handle both direct array and wrapped object responses
    if isinstance(parsed, list):
        return parsed
    elif isinstance(parsed, dict):
        # Find the first list value in the dict — that's our annotations
        for value in parsed.values():
            if isinstance(value, list):
                return value

    logger.warning(f"Unexpected response structure: {type(parsed)}")
    return []


def save_annotations(annotations: list, engine) -> int:
    """
    Save annotation results back to silver.reviews by updating existing rows.
    Returns the count of successfully saved annotations.
    """
    if not annotations:
        return 0

    saved = 0
    with engine.connect() as conn:
        for ann in annotations:
            try:
                conn.execute(text("""
                    UPDATE silver.reviews
                    SET sentiment_label       = :sentiment,
                        annotation_confidence = :confidence,
                        primary_topic         = :topic,
                        needs_human_review    = :needs_review
                    WHERE review_id = :review_id
                """), {
                    "sentiment":    ann.get("sentiment"),
                    "confidence":   float(ann.get("confidence", 0.0)),
                    "topic":        ann.get("primary_topic"),
                    "needs_review": bool(ann.get("needs_human_review", False)),
                    "review_id":    ann.get("review_id"),
                })
                saved += 1
            except Exception as e:
                logger.error(f"Failed to save annotation for "
                             f"{ann.get('review_id')}: {e}")
        conn.commit()

    return saved


def add_annotation_columns(engine):
    """Add extra annotation columns to silver.reviews if not already present."""
    logger.info("Ensuring annotation columns exist in silver.reviews...")
    with engine.connect() as conn:
        for col_sql in [
            "ALTER TABLE silver.reviews ADD COLUMN IF NOT EXISTS "
            "annotation_confidence FLOAT",
            "ALTER TABLE silver.reviews ADD COLUMN IF NOT EXISTS "
            "primary_topic VARCHAR(50)",
            "ALTER TABLE silver.reviews ADD COLUMN IF NOT EXISTS "
            "needs_human_review BOOLEAN",
        ]:
            conn.execute(text(col_sql))
        conn.commit()
    logger.info("Annotation columns ready")


def run_annotation_pipeline():
    """Run the complete annotation pipeline end to end."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key or api_key == "your_openai_key_here":
        raise ValueError(
            "OPENAI_API_KEY not set in .env file. "
            "Get your key from https://platform.openai.com/api-keys"
        )

    engine = get_engine()
    client = OpenAI(api_key=api_key)

    add_annotation_columns(engine)

    reviews_df = load_reviews_to_annotate(engine, limit=MAX_REVIEWS)
    if len(reviews_df) == 0:
        logger.info("No reviews to annotate — all reviews already labelled")
        return

    reviews_list = reviews_df.to_dict("records")
    batches = [
        reviews_list[i:i + BATCH_SIZE]
        for i in range(0, len(reviews_list), BATCH_SIZE)
    ]

    logger.info(f"Processing {len(reviews_list):,} reviews in "
                f"{len(batches)} batches of {BATCH_SIZE}")

    total_saved = 0
    total_errors = 0

    for batch_num, batch in enumerate(batches, 1):
        logger.info(f"Batch {batch_num}/{len(batches)} "
                    f"({len(batch)} reviews)...")
        try:
            prompt = build_annotation_prompt(batch)
            annotations = call_openai_api(prompt, client)
            saved = save_annotations(annotations, engine)
            total_saved += saved
            logger.info(f"  Saved {saved} annotations")

        except json.JSONDecodeError as e:
            logger.error(f"  JSON parse error in batch {batch_num}: {e}")
            total_errors += 1

        except Exception as e:
            logger.error(f"  Error in batch {batch_num}: {e}")
            total_errors += 1

        if batch_num < len(batches):
            time.sleep(BATCH_DELAY)

    logger.info("\n" + "=" * 60)
    logger.info("ANNOTATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  Reviews processed: {len(reviews_list):,}")
    logger.info(f"  Annotations saved: {total_saved:,}")
    logger.info(f"  Batches with errors: {total_errors}")
    logger.info(f"  Success rate: {total_saved/len(reviews_list):.1%}")


def print_annotation_sample(engine):
    """Print a sample of annotated reviews to verify quality."""
    logger.info("\nSample annotated reviews:")
    query = """
        SELECT rating, sentiment_label, annotation_confidence,
               primary_topic, LEFT(review_text, 80) as preview
        FROM silver.reviews
        WHERE sentiment_label IS NOT NULL
        ORDER BY annotation_confidence DESC
        LIMIT 10
    """
    df = pd.read_sql(query, engine)
    for _, row in df.iterrows():
        logger.info(
            f"  [{row['rating']}★] {row['sentiment_label']:<9} "
            f"({row['annotation_confidence']:.2f}) "
            f"[{row['primary_topic']}] "
            f"{row['preview']}"
        )


if __name__ == "__main__":
    run_annotation_pipeline()
    engine = get_engine()
    print_annotation_sample(engine)