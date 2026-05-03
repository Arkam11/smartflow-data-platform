import os
import logging
import pickle
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    classification_report,
    roc_auc_score,
    confusion_matrix,
    accuracy_score
)
from sklearn.preprocessing import StandardScaler
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# The exact columns the model trains on
# These must be identical at training time and scoring time
FEATURE_COLUMNS = [
    "total_orders",
    "avg_days_to_deliver",
    "late_delivery_rate",
    "total_spend",
    "avg_order_value",
    "avg_rating",
    "review_count",
    "days_since_last_order",
    "customer_lifetime_days",
    "payment_type_encoded",
    "state_encoded",
]

MODEL_PATH = "models/churn_model.pkl"
SCALER_PATH = "models/churn_scaler.pkl"


def get_engine():
    db_url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}"
        f"/{os.getenv('DB_NAME')}"
    )
    return create_engine(db_url)


def load_feature_store(engine) -> pd.DataFrame:
    """Load the feature store built in the previous step."""
    df = pd.read_sql("SELECT * FROM ml.feature_store", engine)
    logger.info(f"Loaded feature store — {len(df):,} rows")
    return df


def prepare_training_data(features: pd.DataFrame):
    """
    Split features into X (inputs) and y (label to predict).
    Then split into training set and test set.

    Why split into train/test?
    The model learns patterns from the training set.
    We test it on data it has NEVER seen — the test set.
    This tells us how well it will perform on future, unknown customers.
    If we tested on training data, the model would look artificially good
    because it already memorised those examples.

    80% train / 20% test is the standard split.
    random_state=42 makes the split reproducible — same split every run.
    """
    logger.info("Preparing training data...")

    X = features[FEATURE_COLUMNS].copy()
    y = features["churned"].copy()

    logger.info(f"  Features (X): {X.shape}")
    logger.info(f"  Label (y): {y.shape} — "
                f"{y.sum():,} churned, {(y==0).sum():,} active")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
        # stratify=y ensures the churn ratio is preserved in both splits
        # Without this, you might get all churned customers in train
        # and none in test — making evaluation misleading
    )

    logger.info(f"  Training set: {len(X_train):,} rows")
    logger.info(f"  Test set: {len(X_test):,} rows")

    return X_train, X_test, y_train, y_test


def train_model(X_train, y_train):
    """
    Train the RandomForestClassifier.

    Key hyperparameters explained:
    - n_estimators=100  : number of trees in the forest — more trees = more stable
    - max_depth=10      : how deep each tree can grow — prevents overfitting
    - min_samples_leaf=5: minimum samples in a leaf node — prevents tiny splits
    - class_weight='balanced': compensates for imbalanced classes
                               (more active customers than churned)
    - random_state=42   : reproducibility
    """
    logger.info("Training RandomForestClassifier...")

    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        min_samples_leaf=5,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1  # use all CPU cores
    )

    model.fit(X_train, y_train)
    logger.info("Model training complete")
    return model


def evaluate_model(model, X_test, y_test):
    """
    Evaluate the model on the test set and log key metrics.

    Metrics explained:
    - Accuracy     : % of predictions that were correct overall
    - AUC-ROC      : how well the model ranks churned vs active customers
                     0.5 = random guessing, 1.0 = perfect
    - Precision    : of customers predicted as churned, what % actually churned
    - Recall       : of all customers who actually churned, what % did we catch
    - F1 score     : harmonic mean of precision and recall
    """
    logger.info("Evaluating model on test set...")

    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]  # probability of churn

    accuracy = accuracy_score(y_test, y_pred)
    auc = roc_auc_score(y_test, y_prob)

    logger.info(f"  Accuracy:  {accuracy:.3f} ({accuracy:.1%})")
    logger.info(f"  AUC-ROC:   {auc:.3f}")

    logger.info("\nClassification Report:")
    report = classification_report(y_test, y_pred,
                                    target_names=["Active", "Churned"])
    for line in report.split("\n"):
        if line.strip():
            logger.info(f"  {line}")

    # Feature importance — which features matter most
    importance_df = pd.DataFrame({
        "feature": FEATURE_COLUMNS,
        "importance": model.feature_importances_
    }).sort_values("importance", ascending=False)

    logger.info("\nTop feature importances:")
    for _, row in importance_df.head(5).iterrows():
        bar = "█" * int(row["importance"] * 50)
        logger.info(f"  {row['feature']:<30} {row['importance']:.3f} {bar}")

    return {
        "accuracy": accuracy,
        "auc_roc": auc,
        "feature_importance": importance_df
    }


def save_model(model, scaler=None):
    """Save the trained model to disk as a pickle file."""
    os.makedirs("models", exist_ok=True)
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(model, f)
    logger.info(f"Model saved to {MODEL_PATH}")


def load_model():
    """Load a previously trained model from disk."""
    with open(MODEL_PATH, "rb") as f:
        model = pickle.load(f)
    logger.info(f"Model loaded from {MODEL_PATH}")
    return model


def score_all_customers(model, features: pd.DataFrame) -> pd.DataFrame:
    """
    Score every customer with a churn probability.

    predict_proba() returns two columns:
    - column 0: probability of NOT churning (active)
    - column 1: probability of churning ← this is what we want

    The score is between 0.0 and 1.0:
    - 0.0 = very unlikely to churn
    - 1.0 = very likely to churn
    """
    logger.info("Scoring all customers...")

    X = features[FEATURE_COLUMNS].copy()
    churn_probabilities = model.predict_proba(X)[:, 1]
    churn_predictions = model.predict(X)

    scores = pd.DataFrame({
        "customer_key":  features["customer_key"],
        "churn_score":   churn_probabilities.round(4),
        "churn_risk":    pd.cut(
            churn_probabilities,
            bins=[0, 0.3, 0.6, 1.0],
            labels=["low", "medium", "high"]
        ),
        "predicted_churn": churn_predictions,
        "actual_churned":  features["churned"],
    })

    # Log distribution of risk tiers
    risk_dist = scores["churn_risk"].value_counts()
    logger.info("Churn risk distribution:")
    for tier, count in risk_dist.items():
        logger.info(f"  {tier:<10} {count:,} customers "
                    f"({count/len(scores):.1%})")

    return scores


def save_scores(scores: pd.DataFrame, engine):
    """Save customer churn scores back to PostgreSQL with CASCADE drop."""
    logger.info("Saving churn scores to ml.churn_scores...")

    # Drop with CASCADE first to remove dependent dbt views
    from sqlalchemy import text
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS ml.churn_scores CASCADE"))
        conn.commit()

    scores.to_sql(
        name="churn_scores",
        con=engine,
        schema="ml",
        if_exists="replace",
        index=False,
        chunksize=1000
    )
    logger.info(f"Saved {len(scores):,} customer scores to ml.churn_scores")

def run_churn_pipeline():
    """Run the complete churn prediction pipeline end to end."""
    logger.info("=" * 60)
    logger.info("Starting churn prediction pipeline")
    logger.info("=" * 60)

    engine = get_engine()

    # Step 1: Load features
    features = load_feature_store(engine)

    # Step 2: Train/test split
    X_train, X_test, y_train, y_test = prepare_training_data(features)

    # Step 3: Train model
    model = train_model(X_train, y_train)

    # Step 4: Evaluate
    metrics = evaluate_model(model, X_test, y_test)

    # Step 5: Save model to disk
    save_model(model)

    # Step 6: Score all customers
    scores = score_all_customers(model, features)

    # Step 7: Save scores to database
    save_scores(scores, engine)

    logger.info("\n" + "=" * 60)
    logger.info("Churn pipeline complete")
    logger.info(f"  AUC-ROC: {metrics['auc_roc']:.3f}")
    logger.info(f"  Accuracy: {metrics['accuracy']:.3f}")
    logger.info("=" * 60)

    return metrics


if __name__ == "__main__":
    run_churn_pipeline()