import os
import joblib

from pyspark.sql import SparkSession
from sklearn.ensemble import IsolationForest


# ==========================================================
# CONFIGURATION
# ==========================================================

SILVER_PATH = "data/silver"
MODEL_PATH = "models/isolation_forest.pkl"


# ==========================================================
# START SPARK
# ==========================================================

spark = (
    SparkSession.builder
    .appName("IsolationForestTraining")
    .master("local[*]")
    .getOrCreate()
)


# ==========================================================
# LOAD SILVER DATA
# ==========================================================

df = spark.read.parquet(SILVER_PATH)


# ==========================================================
# USE ONLY VALID TRANSACTIONS
# ==========================================================

normal_df = (
    df
    .filter("quality_status = 'VALID'")
    .select("amount")
    .dropna()
)


print("\n========================================")
print("ISOLATION FOREST TRAINING")
print("========================================")

training_count = normal_df.count()

print(f"Training records: {training_count}")


# ==========================================================
# CONVERT TO NUMPY
# ==========================================================

amounts = normal_df.toPandas()[["amount"]]


# ==========================================================
# TRAIN ISOLATION FOREST
# ==========================================================

model = IsolationForest(
    n_estimators=100,
    contamination=0.05,
    random_state=42
)

model.fit(amounts)


# ==========================================================
# SAVE MODEL
# ==========================================================

os.makedirs(
    os.path.dirname(MODEL_PATH),
    exist_ok=True
)

joblib.dump(
    model,
    MODEL_PATH
)

print(f"Model saved to: {MODEL_PATH}")


# ==========================================================
# TEST MODEL ON TRAINING DATA
# ==========================================================

predictions = model.predict(amounts)

anomaly_count = (predictions == -1).sum()

print(
    f"Detected anomalies in training data: {anomaly_count}"
)

print("========================================\n")


spark.stop()
