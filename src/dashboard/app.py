import streamlit as st
import pandas as pd

# --------------------------------------------------
# PAGE CONFIG
# --------------------------------------------------

st.set_page_config(
    page_title="Real-Time Data Quality Monitor",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Real-Time Data Quality Monitor")
st.caption("Kafka + Spark Structured Streaming + Isolation Forest")

# --------------------------------------------------
# PATHS
# --------------------------------------------------

SILVER_PATH = "dashboard_data/silver"
METRICS_PATH = "dashboard_data/metrics"

# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------

@st.cache_data(ttl=5)
def load_data():

    silver_df = pd.read_parquet(SILVER_PATH)
    metrics_df = pd.read_parquet(METRICS_PATH)

    return silver_df, metrics_df


try:

    silver_df, metrics_df = load_data()

except Exception as e:

    st.error(f"Unable to load data: {e}")
    st.stop()


# --------------------------------------------------
# REFRESH
# --------------------------------------------------

if st.button("🔄 Refresh Dashboard"):

    st.cache_data.clear()
    st.rerun()


# --------------------------------------------------
# KPI CALCULATIONS
# --------------------------------------------------

total_records = len(silver_df)

valid_records = (
    silver_df["quality_status"] == "VALID"
).sum()

invalid_records = (
    silver_df["quality_status"] == "INVALID"
).sum()

duplicate_records = (
    silver_df["quality_status"] == "DUPLICATE"
).sum()

anomaly_records = (
    silver_df["quality_status"] == "ANOMALY"
).sum()


if total_records > 0:
    quality_score = (valid_records / total_records) * 100
else:
    quality_score = 0.0
# --------------------------------------------------
# KPI CARDS
# --------------------------------------------------

st.subheader("📈 Data Quality Overview")

col1, col2, col3, col4, col5, col6 = st.columns(6)

col1.metric(
    "Total Records",
    f"{total_records:,}"
)

col2.metric(
    "Valid Records",
    f"{valid_records:,}"
)

col3.metric(
    "Invalid Records",
    f"{invalid_records:,}"
)

col4.metric(
    "Duplicates",
    f"{duplicate_records:,}"
)

col5.metric(
    "Anomalies",
    f"{anomaly_records:,}"
)

col6.metric(
    "Quality Score",
    f"{quality_score:.2f}%"
)


# --------------------------------------------------
# CHARTS
# --------------------------------------------------

st.divider()

left, right = st.columns(2)


with left:

    st.subheader("📊 Quality Status Distribution")

    status_df = (
        silver_df["quality_status"]
        .value_counts()
        .rename_axis("quality_status")
        .reset_index(name="count")
    )

    st.bar_chart(
        status_df.set_index("quality_status")
    )


with right:

    st.subheader("📈 Quality Score Trend")

    if not metrics_df.empty:

        trend_df = (
            metrics_df
            .sort_values("metric_timestamp")
            .set_index("metric_timestamp")
        )

        st.line_chart(
            trend_df["quality_score"]
        )

    else:

        st.info("No quality metrics available.")


# --------------------------------------------------
# ISOLATION FOREST ANOMALIES
# --------------------------------------------------

st.divider()

st.subheader("🚨 Isolation Forest Anomalies")

anomaly_df = silver_df[
    silver_df["quality_reason"]
    .fillna("")
    .str.contains(
        "Isolation Forest anomaly",
        case=False
    )
]

anomaly_columns = [
    "transaction_id",
    "customer_id",
    "amount",
    "payment_method",
    "location",
    "status",
    "quality_status",
    "quality_reason"
]

if not anomaly_df.empty:

    st.dataframe(
        anomaly_df[anomaly_columns],
        use_container_width=True,
        hide_index=True
    )

else:

    st.info("No Isolation Forest anomalies detected.")


# --------------------------------------------------
# PROBLEMATIC TRANSACTIONS
# --------------------------------------------------

st.divider()

st.subheader("⚠️ Problematic Transactions")

problem_df = silver_df[
    silver_df["quality_status"] != "VALID"
]

if not problem_df.empty:

    st.dataframe(
        problem_df[anomaly_columns],
        use_container_width=True,
        hide_index=True
    )

else:

    st.success(
        "No problematic transactions found."
    )


# --------------------------------------------------
# RECENT TRANSACTIONS
# --------------------------------------------------

st.divider()

st.subheader("🧾 Recent Transactions")

recent_df = silver_df.copy()

if "timestamp" in recent_df.columns:

    recent_df["timestamp"] = pd.to_datetime(
        recent_df["timestamp"]
    )

    recent_df = (
        recent_df
        .sort_values(
            "timestamp",
            ascending=False
        )
        .head(20)
    )

recent_columns = [
    "transaction_id",
    "customer_id",
    "amount",
    "payment_method",
    "location",
    "status",
    "quality_status",
    "quality_reason"
]

st.dataframe(
    recent_df[recent_columns],
    use_container_width=True,
    hide_index=True
)


# --------------------------------------------------
# FOOTER
# --------------------------------------------------

st.divider()

st.caption(
    "Real-Time Data Quality Monitor | "
    "Kafka • Spark Structured Streaming • "
    "PySpark • Isolation Forest • "
    "Parquet • Streamlit"
)
