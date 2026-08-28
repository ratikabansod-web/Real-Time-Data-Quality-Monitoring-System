import os
import joblib


# ==========================================================
# CONFIGURATION
# ==========================================================

MODEL_PATH = "models/isolation_forest.pkl"


# ==========================================================
# LOAD MODEL
# ==========================================================

if not os.path.exists(MODEL_PATH):
    raise FileNotFoundError(
        f"Isolation Forest model not found: {MODEL_PATH}"
    )

model = joblib.load(MODEL_PATH)


# ==========================================================
# PREDICT ANOMALY
# ==========================================================

def predict_anomaly(amount):
    """
    Predict whether a transaction amount is anomalous.

    Returns:
        True  -> anomaly
        False -> normal
    """

    if amount is None:
        return False

    prediction = model.predict([[float(amount)]])

    return prediction[0] == -1
