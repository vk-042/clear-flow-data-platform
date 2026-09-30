"""Read-only operational dashboard; bind to localhost for the portfolio demo."""
import json
from datetime import datetime, timezone
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import select
from clearflow.storage import get_engine, entities, events, raw, reconciliation

st.set_page_config(page_title="ClearFlow | Operations", page_icon="◈", layout="wide")
st.markdown("""<style>
.block-container {padding-top:4rem} .stApp {background:#0b1120}
h1 {letter-spacing:-1.5px} [data-testid="stMetric"] {background:#111e32;padding:18px;border-radius:12px}
</style>""", unsafe_allow_html=True)
st.caption("CLEARFLOW  /  STREAMING DATA PLATFORM")
st.title("Payments & claims. One operational view.")
st.caption("Synthetic data • Banking and healthcare • Refreshes every 5 seconds")
domain = st.sidebar.selectbox("Business domain", ["banking", "healthcare"])
st.sidebar.info("Review flags identify unusual amounts. They do not establish fraud.")

@st.cache_resource
def database():
    return get_engine()

def chart(series, color, key, line=False):
    trace = go.Scatter(x=list(series.index), y=list(series.values), mode="lines+markers",
                       line={"color": color}) if line else go.Bar(
                           x=list(series.index), y=list(series.values), marker_color=color)
    figure = go.Figure(trace)
    figure.update_layout(height=300, margin=dict(l=10,r=10,t=10,b=20),
                         paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                         font_color="#e2e8f0", xaxis_title=None, yaxis_title="Count")
    st.plotly_chart(figure, key=key, use_container_width=True,
                    config={"displayModeBar": False})

@st.fragment(run_every="5s")
def render():
    with database().connect() as conn:
        state = pd.read_sql(select(entities).where(entities.c.domain == domain), conn)
        history = pd.read_sql(select(events).where(events.c.domain == domain), conn)
        outcomes = pd.read_sql(select(raw.c.outcome, raw.c.received_at), conn)
        rec = conn.execute(select(reconciliation.c.report_json).order_by(
            reconciliation.c.created_at.desc()).limit(1)).scalar()
    if state.empty:
        st.info("Waiting for events. Run the demo or start the Kafka producer.")
        return
    latest = pd.to_datetime(history["received_at"], utc=True).max()
    age = (datetime.now(timezone.utc) - latest.to_pydatetime()).total_seconds()
    if age > 30:
        st.warning(f"Last accepted event was {age:.0f}s ago. The source may be idle or processing may be delayed.")
    cols = st.columns(4)
    cols[0].metric("Distinct payments" if domain == "banking" else "Distinct claims", f"{len(state):,}")
    terminal = "settled" if domain == "banking" else "paid"
    value = state.loc[(state.status == terminal) & (state.quality_issue == ""), "amount_cents"].sum() / 100
    cols[1].metric(f"Currently {terminal} · USD", f"${value:,.2f}")
    cols[2].metric("Lifecycle issues", int((state.quality_issue != "").sum()))
    cols[3].metric("Entities flagged", history.loc[history.is_anomaly == 1, "entity_id"].nunique())
    left, right = st.columns(2)
    with left:
        st.subheader("Current lifecycle status")
        chart(state.status.value_counts(), "#38bdf8", f"status-{domain}")
    with right:
        st.subheader("Events by business minute")
        times = pd.to_datetime(history.event_time, utc=True).dt.floor("min")
        counts = times.value_counts().sort_index()
        counts.index = counts.index.strftime("%H:%M UTC")
        chart(counts, "#2dd4bf", f"events-{domain}", line=True)
    tabs = st.tabs(["Current state", "Review queue", "Pipeline quality", "Reconciliation"])
    with tabs[0]:
        st.dataframe(state.drop(columns=["party_id"]), hide_index=True, use_container_width=True)
    with tabs[1]:
        flagged = history[history.is_anomaly == 1].sort_values("sequence").drop_duplicates("entity_id", keep="last")
        st.dataframe(flagged[["entity_id", "amount_cents", "anomaly_score", "model_version"]],
                     hide_index=True, use_container_width=True)
    with tabs[2]:
        st.caption("Ingestion outcomes cover both domains. Event delay includes simulated source delay.")
        chart(outcomes.outcome.value_counts(), "#38bdf8", f"quality-{domain}")
        delay = (pd.to_datetime(history.received_at, utc=True) - pd.to_datetime(history.event_time, utc=True)).dt.total_seconds()
        st.metric("P95 event-to-ingestion delay", f"{delay.quantile(.95):.2f}s")
        st.caption("This is not Kafka consumer lag or a verified end-to-end streaming latency benchmark.")
    with tabs[3]:
        st.json(json.loads(rec) if rec else {"message": "No reconciliation run yet"})
render()
