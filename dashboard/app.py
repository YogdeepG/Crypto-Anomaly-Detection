
import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from pathlib import Path


# ==========================================
# DATABASE CONFIGURATION
# ==========================================

DATABASE_PATH = (
    Path(__file__).resolve().parent.parent
    / "data"
    / "crypto_trades.db"
)

REFRESH_INTERVAL = "5s"


# ==========================================
# PAGE CONFIGURATION
# ==========================================

st.set_page_config(
    page_title="Crypto Anomaly Detection",
    page_icon="📈",
    layout="wide"
)


# ==========================================
# CUSTOM DASHBOARD DESIGN
# ==========================================

st.markdown("""
<style>

.main {
    background-color: #0e1117;
}

[data-testid="stMetric"] {
    background-color: #1c2333;
    padding: 20px;
    border-radius: 12px;
    border: 1px solid #30394d;
}

[data-testid="stMetricValue"] {
    color: #00d4aa;
}

h1 {
    color: #ffffff;
}

h2, h3 {
    color: #e6e6e6;
}

</style>
""", unsafe_allow_html=True)


# ==========================================
# DATABASE LOADING FUNCTION
# ==========================================

def load_data():

    if not DATABASE_PATH.exists():
        return pd.DataFrame(), pd.DataFrame(), None

    connection = sqlite3.connect(DATABASE_PATH)

    try:

        trades = pd.read_sql_query(
            """
            SELECT *
            FROM trades
            WHERE processing_timestamp >= (
                SELECT COALESCE(MAX(processing_timestamp), 0) - 300000
                FROM trades
            )
            ORDER BY processing_timestamp ASC
            """,
            connection
        )

        # Check which columns exist in alerts table
        alert_columns = pd.read_sql_query(
            "PRAGMA table_info(alerts)",
            connection
        )["name"].tolist()

        timestamp_column = (
            "alert_timestamp"
            if "alert_timestamp" in alert_columns
            else "processing_timestamp"
            if "processing_timestamp" in alert_columns
            else None
        )

        if timestamp_column:

            alerts = pd.read_sql_query(
                f"""
                SELECT *
                FROM alerts
                WHERE {timestamp_column} >= (
                    SELECT COALESCE(MAX(processing_timestamp), 0) - 300000
                    FROM trades
                )
                ORDER BY {timestamp_column} ASC
                """,
                connection
            )

        else:
            alerts = pd.DataFrame()

    finally:
        connection.close()

    return trades, alerts, timestamp_column


# ==========================================
# DASHBOARD
# Refreshes automatically every 5 seconds
# ==========================================

@st.fragment(run_every=REFRESH_INTERVAL)
def dashboard():

    st.title("📈 Real-Time Cryptocurrency Anomaly Detection")

    st.caption(
        "Live BTC/USDT trade monitoring and intelligent anomaly detection"
    )

    trades, alerts, alert_time_column = load_data()

    # ======================================
    # DATABASE STATUS
    # ======================================

    if not DATABASE_PATH.exists():

        st.error(
            f"Database not found: {DATABASE_PATH}"
        )
        return

    st.success("Database connected successfully")

    # ======================================
    # METRICS
    # ======================================

    total_trades = len(trades)
    total_alerts = len(alerts)

    col1, col2, col3 = st.columns(3)

    with col1:

        st.metric(
            "Total Trades",
            f"{total_trades:,}"
        )

    with col2:

        st.metric(
            "Anomaly Alerts",
            f"{total_alerts:,}",
            delta="Detected anomalies"
        )

    with col3:

        if not trades.empty:

            latest_price = float(
                trades.iloc[-1]["price"]
            )

            st.metric(
                "Latest BTC Price",
                f"${latest_price:,.2f}"
            )

        else:

            st.metric(
                "Latest BTC Price",
                "No data"
            )

    st.caption(
        "Statistics represent the latest rolling 5-minute window. "
        "Dashboard refreshes every 5 seconds."
    )

    st.divider()

    # ======================================
    # BTC PRICE MOVEMENT CHART
    # ======================================

    st.subheader("📊 BTC/USDT Price Movement")

    if not trades.empty:

        chart_data = trades.copy()

        chart_data["time"] = pd.to_datetime(
            chart_data["processing_timestamp"],
            unit="ms",
            utc=True
        ).dt.tz_convert("Europe/Berlin")

        chart_data = chart_data.sort_values("time")

        fig = px.line(
            chart_data,
            x="time",
            y="price",
            labels={
                "time": "Time (Hamburg)",
                "price": "BTC Price (USDT)"
            },
            template="plotly_dark"
        )

        # Highlight anomaly points in red
        if (
            not alerts.empty
            and alert_time_column
            and "price" in alerts.columns
        ):

            alert_data = alerts.copy()

            alert_data["time"] = pd.to_datetime(
                alert_data[alert_time_column],
                unit="ms",
                utc=True
            ).dt.tz_convert("Europe/Berlin")

            fig.add_trace(
                go.Scatter(
                    x=alert_data["time"],
                    y=alert_data["price"],
                    mode="markers",
                    name="Anomaly Alerts",
                    marker=dict(
                        color="red",
                        size=9,
                        symbol="x"
                    ),
                    hovertemplate=(
                        "Anomaly Price: %{y:,.2f} USDT"
                        "<extra></extra>"
                    )
                )
            )

        fig.update_layout(
            height=500,
            hovermode="x unified",
            xaxis_title="Time (Hamburg)",
            yaxis_title="BTC Price (USDT)",
            legend_title="Detection",
            margin=dict(
                l=20,
                r=20,
                t=30,
                b=20
            )
        )

        fig.update_traces(
            line=dict(width=2)
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )

    else:

        st.info(
            "Waiting for incoming cryptocurrency trades..."
        )

    st.divider()

    # ======================================
    # RECENT ANOMALY ALERTS
    # ======================================

    st.subheader("🚨 Recent Anomaly Alerts")

    if not alerts.empty:

        recent_alerts = alerts.copy()

        if alert_time_column:

            recent_alerts["Time (Hamburg)"] = (
                pd.to_datetime(
                    recent_alerts[alert_time_column],
                    unit="ms",
                    utc=True
                )
                .dt.tz_convert("Europe/Berlin")
                .dt.strftime("%Y-%m-%d %H:%M:%S")
            )

        display_columns = [
            column
            for column in [
                "Time (Hamburg)",
                "symbol",
                "price",
                "quantity",
                "volume",
                "anomalies",
                "is_anomaly",
                "anomaly_score"
            ]
            if column in recent_alerts.columns
        ]

        st.dataframe(
            recent_alerts[
                display_columns
            ].sort_values(
                "Time (Hamburg)",
                ascending=False
            ).head(10)
            if "Time (Hamburg)" in display_columns
            else recent_alerts[display_columns].head(10),
            use_container_width=True,
            hide_index=True
        )

        st.warning(
            f"{total_alerts} anomalies detected in the latest 5-minute window."
        )

    else:

        st.success(
            "No anomalies detected in the current window."
        )

    st.divider()

    # ======================================
    # RECENT TRADES
    # ======================================

    st.subheader("📋 Recent Cryptocurrency Trades")

    if not trades.empty:

        recent_trades = trades.tail(10).copy()

        recent_trades["Time (Hamburg)"] = (
            pd.to_datetime(
                recent_trades["processing_timestamp"],
                unit="ms",
                utc=True
            )
            .dt.tz_convert("Europe/Berlin")
            .dt.strftime("%Y-%m-%d %H:%M:%S")
        )

        display_columns = [
            column
            for column in [
                "Time (Hamburg)",
                "symbol",
                "price",
                "quantity",
                "is_anomaly",
                "anomalies"
            ]
            if column in recent_trades.columns
        ]

        st.dataframe(
            recent_trades[display_columns].iloc[::-1],
            use_container_width=True,
            hide_index=True
        )

    else:

        st.info("No recent trades available.")

    # ======================================
    # FOOTER
    # ======================================

    st.divider()

    st.caption(
        "Crypto Anomaly Detection System | "
        "Powered by Python, Kafka, SQLite and Streamlit"
    )


# ==========================================
# RUN DASHBOARD
# ==========================================

dashboard()
