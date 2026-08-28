import json
import os
from datetime import datetime, timezone
from sklearn.ensemble import IsolationForest
from src.anomaly.isolation_forest import detect_amount_anomalies

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    lit,
    when,
    concat_ws,
    udf
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)


# ==========================================================
# CONFIGURATION
# ==========================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "business_transactions"

STATE_FILE = "/tmp/dqm_processed_transactions.json"

BRONZE_PATH = "data/bronze"
SILVER_PATH = "data/silver"
QUALITY_RESULTS_PATH = "data/quality_results"
METRICS_PATH = "data/metrics"

CHECKPOINT_PATH = "/tmp/dqm_duplicate_checkpoint"


# ==========================================================
# EXPECTED TRANSACTION FIELDS
# ==========================================================

EXPECTED_FIELDS = {
    "transaction_id",
    "customer_id",
    "amount",
    "payment_method",
    "location",
    "transaction_timestamp",
    "status"
}


# ==========================================================
# LOAD PROCESSED TRANSACTION IDs
# ==========================================================

def load_processed_ids():

    if not os.path.exists(STATE_FILE):
        return set()

    try:

        with open(STATE_FILE, "r") as file:
            return set(json.load(file))

    except Exception:

        return set()


# ==========================================================
# SAVE PROCESSED TRANSACTION IDs
# ==========================================================

def save_processed_ids(processed_ids):

    with open(STATE_FILE, "w") as file:

        json.dump(
            list(processed_ids),
            file
        )


# ==========================================================
# SCHEMA VALIDATION
# ==========================================================

def validate_schema(json_string):

    try:

        record = json.loads(json_string)

        actual_fields = set(record.keys())

        missing_fields = EXPECTED_FIELDS - actual_fields

        extra_fields = actual_fields - EXPECTED_FIELDS

        if missing_fields:

            return (
                f"Missing fields: "
                f"{', '.join(sorted(missing_fields))}"
            )

        if extra_fields:

            return (
                f"Unexpected fields: "
                f"{', '.join(sorted(extra_fields))}"
            )

        return None

    except Exception:

        return "Malformed JSON"


schema_validation_udf = udf(
    validate_schema,
    StringType()
)


# ==========================================================
# PROCESS EACH MICRO-BATCH
# ==========================================================

def process_batch(batch_df, batch_id):

    # ------------------------------------------------------
    # Ignore empty batches
    # ------------------------------------------------------

    if batch_df.isEmpty():

        return

    # ======================================================
    # ISOLATION FOREST ANOMALY DETECTION
    # ======================================================

    ml_anomalies = detect_amount_anomalies(batch_df)

    batch_df = batch_df.withColumn(
        "ml_anomaly",
        col("transaction_id").isin(list(ml_anomalies))
    )


    # ======================================================
    # BRONZE LAYER
    # Store raw Kafka records
    # ======================================================

    bronze_df = batch_df.select(
        "topic",
        "partition",
        "offset",
        "timestamp",
        "raw_json"
    ).withColumn(
        "batch_id",
        lit(batch_id)
    )

    bronze_df.write.mode("append").parquet(
        BRONZE_PATH
    )


    # ======================================================
    # LOAD PROCESSED IDS
    # ======================================================

    processed_ids = load_processed_ids()


    # ======================================================
    # DUPLICATE DETECTION
    # ======================================================

    batch_with_duplicates = (

        batch_df

        .withColumn(
            "duplicate",

            when(

                col("transaction_id").isin(
                    list(processed_ids)
                ),

                lit(True)

            ).otherwise(
                lit(False)
            )
        )
    )


    # ======================================================
    # QUALITY STATUS
    # ======================================================

    quality_result = (

        batch_with_duplicates

        .withColumn(
            "quality_status",

            when(
                col("duplicate"),
                lit("DUPLICATE")
            )

            .when(
                col("schema_error").isNotNull(),
                lit("SCHEMA_VIOLATION")
            )

            .when(
                col("missing_field").isNotNull(),
                lit("INVALID")
            )

            .when(
                col("invalid_amount"),
                lit("INVALID")
            )

            .when(
                col("invalid_payment_method"),
                lit("INVALID")
            )

            .when(
                col("invalid_status"),
                lit("INVALID")
            )

            .when(
                col("amount_anomaly") | col("ml_anomaly"),
                lit("ANOMALY")
            )

            .otherwise(
                lit("VALID")
            )
        )

        .withColumn(
            "quality_reason",

            concat_ws(

                ", ",

                when(
                    col("duplicate"),
                    lit("Duplicate transaction_id")
                ),

                when(
                    col("schema_error").isNotNull(),
                    col("schema_error")
                ),

                when(
                    col("missing_field").isNotNull(),

                    concat_ws(
                        ": ",
                        lit("Missing value"),
                        col("missing_field")
                    )
                ),

                when(
                    col("invalid_amount"),
                    lit("Invalid amount")
                ),

                when(
                    col("invalid_payment_method"),
                    lit("Invalid payment method")
                ),

                when(
                    col("invalid_status"),
                    lit("Invalid status")
                ),

                when(
                    col("amount_anomaly"),
                    lit("High amount anomaly")
                ),
		when(
   		    col("ml_anomaly"),
   		    lit("Isolation Forest anomaly")
		)
            )
        )
    )


    # ======================================================
    # DISPLAY QUALITY RESULTS
    # ======================================================

    result = quality_result.select(

        "transaction_id",
        "customer_id",
        "amount",
        "payment_method",
        "location",
        "status",
        "quality_status",
        "quality_reason"

    )

    result.show(
        truncate=False
    )


    # ======================================================
    # SILVER LAYER
    # Cleaned + quality-enriched data
    # ======================================================

    silver_output = quality_result.select(

        "transaction_id",
        "customer_id",
        "amount",
        "payment_method",
        "location",
        "transaction_timestamp",
        "status",

        "quality_status",
        "quality_reason",

        "topic",
        "partition",
        "offset",
        "timestamp"

    )

    silver_output.write.mode("append").parquet(
        SILVER_PATH
    )


    # ======================================================
    # QUALITY RESULTS
    # ======================================================

    quality_output = quality_result.select(

        "transaction_id",
        "customer_id",
        "amount",
        "payment_method",
        "location",
        "transaction_timestamp",
        "status",
        "quality_status",
        "quality_reason"

    )

    quality_output.write.mode("append").parquet(
        QUALITY_RESULTS_PATH
    )


    # ======================================================
    # DATA QUALITY METRICS
    # ======================================================

    total_records = quality_result.count()


    valid_records = quality_result.filter(

        col("quality_status") == "VALID"

    ).count()


    invalid_records = quality_result.filter(

        col("quality_status") == "INVALID"

    ).count()


    duplicate_records = quality_result.filter(

        col("quality_status") == "DUPLICATE"

    ).count()


    schema_violations = quality_result.filter(

        col("quality_status") == "SCHEMA_VIOLATION"

    ).count()


    anomalies = quality_result.filter(

    col("quality_reason").contains("High amount anomaly") |

    col("quality_reason").contains("Isolation Forest anomaly")

    ).count()


    # ======================================================
    # QUALITY SCORE
    # ======================================================

    if total_records > 0:

        quality_score = (
            valid_records / total_records
        ) * 100

    else:

        quality_score = 0


    # ======================================================
    # PERSIST QUALITY METRICS
    # ======================================================

    metrics_data = [

        (

            datetime.now(
                timezone.utc
            ).isoformat(),

            total_records,

            valid_records,

            invalid_records,

            duplicate_records,

            schema_violations,

            anomalies,

            float(
                quality_score
            )
        )
    ]


    metrics_df = (

        batch_df.sparkSession.createDataFrame(

            metrics_data,

            [

                "metric_timestamp",

                "total_records",

                "valid_records",

                "invalid_records",

                "duplicate_records",

                "schema_violations",

                "anomalies",

                "quality_score"

            ]
        )
    )


    metrics_df.write.mode("append").parquet(
        METRICS_PATH
    )


    # ======================================================
    # PRINT METRICS
    # ======================================================

    print("\n")

    print("=" * 50)

    print(
        "        REAL-TIME DATA QUALITY METRICS"
    )

    print("=" * 50)

    print(
        f"Total Records       : {total_records}"
    )

    print(
        f"Valid Records       : {valid_records}"
    )

    print(
        f"Invalid Records     : {invalid_records}"
    )

    print(
        f"Duplicate Records   : {duplicate_records}"
    )

    print(
        f"Schema Violations   : {schema_violations}"
    )

    print(
        f"Anomalies           : {anomalies}"
    )

    print(
        f"Data Quality Score  : {quality_score:.2f}%"
    )

    print("=" * 50)

    print("\n")


    # ======================================================
    # UPDATE PROCESSED TRANSACTION STATE
    # ======================================================

    new_ids = (

        batch_df

        .select(
            "transaction_id"
        )

        .where(
            col("transaction_id").isNotNull()
        )

        .distinct()

        .collect()
    )


    for row in new_ids:

        processed_ids.add(
            row["transaction_id"]
        )


    save_processed_ids(
        processed_ids
    )


# ==========================================================
# MAIN
# ==========================================================

def main():

    # ------------------------------------------------------
    # Spark Session
    # ------------------------------------------------------

    spark = (

        SparkSession.builder

        .appName(
            "RealTimeDataQualityMonitor"
        )

        .master("local[*]")

        .getOrCreate()
    )


    spark.sparkContext.setLogLevel(
        "WARN"
    )


    # ======================================================
    # TRANSACTION SCHEMA
    # ======================================================

    transaction_schema = StructType([

        StructField(
            "transaction_id",
            StringType(),
            True
        ),

        StructField(
            "customer_id",
            StringType(),
            True
        ),

        StructField(
            "amount",
            DoubleType(),
            True
        ),

        StructField(
            "payment_method",
            StringType(),
            True
        ),

        StructField(
            "location",
            StringType(),
            True
        ),

        StructField(
            "transaction_timestamp",
            StringType(),
            True
        ),

        StructField(
            "status",
            StringType(),
            True
        )

    ])


    # ======================================================
    # KAFKA STREAM
    # ======================================================

    kafka_stream = (

        spark.readStream

        .format("kafka")

        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS
        )

        .option(
            "subscribe",
            KAFKA_TOPIC
        )

        .option(
            "startingOffsets",
            "latest"
        )

        .load()
    )


    # ======================================================
    # RAW DATA
    #
    # IMPORTANT:
    # Keep Kafka metadata here.
    # ======================================================

    raw_data = kafka_stream.select(

        col("topic"),

        col("partition"),

        col("offset"),

        col("timestamp"),

        col("value")
        .cast("string")
        .alias("raw_json")

    )


    # ======================================================
    # SCHEMA VALIDATION
    # ======================================================

    schema_checked = (

        raw_data

        .withColumn(

            "schema_error",

            schema_validation_udf(
                col("raw_json")
            )
        )
    )


    # ======================================================
    # PARSE JSON
    # ======================================================

    parsed_data = (

        schema_checked

        .withColumn(

            "data",

            from_json(

                col("raw_json"),

                transaction_schema

            )
        )

        .select(

            "topic",
            "partition",
            "offset",
            "timestamp",

            "raw_json",

            "schema_error",

            "data.*"

        )
    )


    # ======================================================
    # DATA QUALITY RULES
    # ======================================================

    quality_checked = (

        parsed_data

        # --------------------------------------------------
        # Missing values
        # --------------------------------------------------

        .withColumn(

            "missing_field",

            when(
                col("transaction_id").isNull(),
                lit("transaction_id")
            )

            .when(
                col("customer_id").isNull(),
                lit("customer_id")
            )

            .when(
                col("amount").isNull(),
                lit("amount")
            )

            .when(
                col("payment_method").isNull(),
                lit("payment_method")
            )

            .when(
                col("location").isNull(),
                lit("location")
            )

            .when(
                col("transaction_timestamp").isNull(),
                lit("transaction_timestamp")
            )

            .when(
                col("status").isNull(),
                lit("status")
            )
        )


        # --------------------------------------------------
        # Invalid amount
        # --------------------------------------------------

        .withColumn(

            "invalid_amount",

            when(

                (col("amount").isNotNull()) &

                (col("amount") <= 0),

                lit(True)

            ).otherwise(
                lit(False)
            )
        )


        # --------------------------------------------------
        # Invalid payment method
        # --------------------------------------------------

        .withColumn(

            "invalid_payment_method",

            when(

                col("payment_method").isNotNull() &

                ~col("payment_method").isin(

                    "UPI",
                    "CARD",
                    "NETBANKING",
                    "CASH"

                ),

                lit(True)

            ).otherwise(
                lit(False)
            )
        )


        # --------------------------------------------------
        # Invalid status
        # --------------------------------------------------

        .withColumn(

            "invalid_status",

            when(

                col("status").isNotNull() &

                ~col("status").isin(

                    "SUCCESS",
                    "FAILED",
                    "PENDING"

                ),

                lit(True)

            ).otherwise(
                lit(False)
            )
        )


        # --------------------------------------------------
        # Amount anomaly
        # --------------------------------------------------

        .withColumn(

            "amount_anomaly",

            when(

                col("amount") > 100000,

                lit(True)

            ).otherwise(
                lit(False)
            )
        )
    )


    # ======================================================
    # START STREAMING
    # ======================================================

    query = (

        quality_checked

        .writeStream

        .foreachBatch(
            process_batch
        )

        .option(
            "checkpointLocation",
            CHECKPOINT_PATH
        )

        .start()
    )


    query.awaitTermination()


# ==========================================================
# ENTRY POINT
# ==========================================================

if __name__ == "__main__":

    main()
