import joblib


MODEL_PATH = "models/isolation_forest.pkl"


def detect_amount_anomalies(batch_df):

    """
    Detect unusual transaction amounts using
    the already-trained Isolation Forest model.

    Returns a set of transaction IDs
    identified as anomalies.
    """

    # ------------------------------------------------------
    # LOAD TRAINED MODEL
    # ------------------------------------------------------

    model = joblib.load(MODEL_PATH)

    # ------------------------------------------------------
    # SELECT ONLY REQUIRED COLUMNS
    # ------------------------------------------------------

    pdf = batch_df.select(
        "transaction_id",
        "amount"
    ).toPandas()

    # ------------------------------------------------------
    # REMOVE NULL AMOUNTS
    # ------------------------------------------------------

    pdf = pdf.dropna(
        subset=["amount"]
    )

    # ------------------------------------------------------
    # HANDLE EMPTY BATCH
    # ------------------------------------------------------

    if len(pdf) == 0:
        return set()

    # ------------------------------------------------------
    # PREDICT USING SAVED MODEL
    # ------------------------------------------------------

    predictions = model.predict(
        pdf[["amount"]]
    )

    pdf["prediction"] = predictions

    # ------------------------------------------------------
    # GET ANOMALOUS TRANSACTION IDs
    # Isolation Forest:
    # -1 = anomaly
    #  1 = normal
    # ------------------------------------------------------

    anomalies = pdf[
        pdf["prediction"] == -1
    ]["transaction_id"]

    return set(
        anomalies.tolist()
    )
