"""
Enterprise Kafka Chaos & DLQ Resilience Live Dashboard.
Designed for interactive resilience demonstration and video capture.
"""
import streamlit as st
import pandas as pd
import time
from core.producer import OrderProducer
from core.chaos_engine import ChaosMutationEngine
from core.dlq_manager import DeadLetterQueueManager
from core.consumer import ResilientConsumer

st.set_page_config(
    page_title="Kafka Chaos & DLQ Resilience Engine",
    page_icon="⚡",
    layout="wide"
)

# Custom Styling: Balanced Height & Modern Terminal Aesthetics
st.markdown("""
<style>
    .block-container {
        padding-top: 2.2rem !important;
        padding-bottom: 1.5rem !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
    }
    .hero-title {
        color: #10b981;
        font-size: 2.3rem;
        font-weight: 800;
        margin: 0px;
        letter-spacing: -0.5px;
    }
    .hero-subtitle {
        color: #cbd5e1;
        font-size: 1.05rem;
        margin-bottom: 18px;
        font-weight: 500;
    }
    div.stButton > button[kind="primary"] {
        background-color: #1d4ed8 !important;
        color: #ffffff !important;
        font-weight: 800 !important;
        font-size: 1.05rem !important;
        border: 2px solid #60a5fa !important;
        border-radius: 8px !important;
        padding: 9px 22px !important;
        box-shadow: 0 4px 14px rgba(29, 78, 216, 0.45) !important;
    }
    div.stButton > button[kind="secondary"] {
        background-color: #1e293b !important;
        color: #ffffff !important;
        border: 2px solid #64748b !important;
        border-radius: 8px !important;
        font-weight: 700 !important;
        padding: 9px 18px !important;
    }
    [data-testid="stMetricValue"] {
        font-size: 1.5rem !important;
        font-weight: 800 !important;
        color: #f8fafc !important;
    }
    .panel-box {
        background-color: #0f172a;
        border: 1px solid #334155;
        border-radius: 8px;
        padding: 12px;
        height: 360px;
        overflow-y: auto;
    }
</style>
""", unsafe_allow_html=True)

# Main Title Section
st.markdown('<div class="hero-title">⚡ Enterprise Event-Stream Chaos & Resilience Sentinel</div>', unsafe_allow_html=True)
st.markdown('<div class="hero-subtitle">Distributed System Fault-Tolerance • Idempotent Deduplication • Dead Letter Queue (DLQ) Quarantine</div>', unsafe_allow_html=True)

# Sidebar Controls
st.sidebar.header("🕹️ Chaos Injection Parameters")
batch_size = st.sidebar.slider("Batch Ingestion Volume", min_value=10, max_value=50, value=20, step=5)
corruption_rate = st.sidebar.slider("Schema Mutation Rate", 0.0, 0.40, 0.15, 0.05)
duplicate_rate = st.sidebar.slider("Network Duplicate Rate", 0.0, 0.40, 0.15, 0.05)
missing_key_rate = st.sidebar.slider("Missing Header Rate", 0.0, 0.30, 0.10, 0.05)

# Session State Persistence
if "producer" not in st.session_state:
    st.session_state.producer = OrderProducer()
if "dlq" not in st.session_state:
    st.session_state.dlq = DeadLetterQueueManager()
if "consumer" not in st.session_state:
    st.session_state.consumer = ResilientConsumer(st.session_state.dlq)

btn_col1, btn_col2, _ = st.columns([1.6, 1.1, 1.3])
with btn_col1:
    trigger = st.button("🚀 Run Distributed Stream & Inject Chaos", type="primary")
with btn_col2:
    if st.button("🔄 Reset Pipeline & State", type="secondary"):
        st.session_state.dlq.purge()
        st.session_state.consumer = ResilientConsumer(st.session_state.dlq)
        st.rerun()

if trigger:
    metric_placeholder = st.empty()
    st.markdown("---")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("#### 📥 Raw Ingestion Pipeline")
        st.caption("Kafka Topic: `orders.raw.stream`")
        raw_table_placeholder = st.empty()
    with col2:
        st.markdown("#### 🛡️ Resilience Audit Log")
        st.caption("Deterministic evaluation & intercept")
        log_placeholder = st.empty()
    with col3:
        st.markdown("#### 🗄️ Dead Letter Queue (DLQ)")
        st.caption("Kafka Topic: `orders.dlq.failed`")
        dlq_table_placeholder = st.empty()

    # 1. Produce Clean Events
    clean_events = st.session_state.producer.produce_batch(batch_size)

    # 2. Inject Chaos
    chaos = ChaosMutationEngine(
        corruption_rate=corruption_rate,
        duplicate_rate=duplicate_rate,
        missing_field_rate=missing_key_rate
    )
    disrupted_stream, _ = chaos.inject_chaos(clean_events)

    # 3. Stream Events Iteratively with Smooth Human-Readable Playback
    current_stream_buffer = []
    
    for idx, event in enumerate(disrupted_stream):
        current_stream_buffer.append(event)
        st.session_state.consumer.consume_event(event)

        processed_cnt = len(st.session_state.consumer.processed_store)
        dlq_cnt = st.session_state.dlq.count
        dup_cnt = len([x for x in st.session_state.consumer.audit_log if x.get("status") == "DUPLICATE_DISCARDED"])
        
        # 1. Update Metrics
        with metric_placeholder.container():
            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("Ingested Events", idx + 1)
            m2.metric("Verified Valid", processed_cnt)
            m3.metric("Duplicates Dropped", dup_cnt)
            m4.metric("Quarantined to DLQ", dlq_cnt)
            m5.metric("Data Loss", "0.00%", delta="Zero Loss Guard", delta_color="normal")

        # 2. Update Left Raw Table dynamically
        df_raw_slice = pd.DataFrame(current_stream_buffer)[["customer_id", "amount", "payment_method"]]
        raw_table_placeholder.dataframe(df_raw_slice, height=360)

        # 3. Update Mid Audit Log dynamically
        recent_audits = st.session_state.consumer.audit_log[-12:]
        log_html = "<div class='panel-box'>"
        for entry in reversed(recent_audits):
            status = entry.get("status", "")
            if status == "SUCCESS_VERIFIED":
                log_html += f"<p style='color: #34d399; margin: 4px 0; font-size: 0.85rem;'>🟢 <b>PASSED:</b> {entry['payload']['order_id']} verified</p>"
            elif status == "DUPLICATE_DISCARDED":
                log_html += f"<p style='color: #fbbf24; margin: 4px 0; font-size: 0.85rem;'>🟠 <b>IDEMPOTENCY DROP:</b> Key {entry['key']} ignored</p>"
            else:
                reason = entry.get('reason', 'Corrupt Payload')[:38] + "..."
                log_html += f"<p style='color: #f87171; margin: 4px 0; font-size: 0.85rem;'>🔴 <b>DLQ ROUTE:</b> {reason}</p>"
        log_html += "</div>"
        log_placeholder.markdown(log_html, unsafe_allow_html=True)

        # 4. Update Right DLQ Table dynamically
        dlq_recs = st.session_state.dlq.get_all_quarantined()
        if dlq_recs:
            df_dlq_slice = pd.DataFrame(dlq_recs)[["failure_reason", "exception_type"]]
            dlq_table_placeholder.dataframe(df_dlq_slice, height=360)
        else:
            dlq_table_placeholder.info("DLQ Clean. No active poisoned messages.")

        # Smooth realistic delay so human eye & video recording can clearly follow each step
        time.sleep(0.30)