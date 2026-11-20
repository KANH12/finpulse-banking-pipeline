import sys
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st


def _load_cloud_secrets() -> None:
    """Expose Streamlit Community Cloud secrets as environment variables."""
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_REGION", "S3_BUCKET"):
        try:
            value = st.secrets.get(name)
        except FileNotFoundError:
            return
        if value:
            os.environ.setdefault(name, str(value))


_load_cloud_secrets()

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from src.dashboard.queries import (
    load_pipeline_health,
    load_error_breakdown,
    load_quarantine_records,
    load_risk_summary,
    load_risk_drivers,
    load_recent_high_risk,
    load_mcc_breakdown,
    load_daily_trend,
)

st.set_page_config(
    page_title="FinPulse",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
/* Typography */
[data-testid="stMetricValue"]  { font-size: 1.85rem; font-weight: 700; letter-spacing: -0.5px; }
[data-testid="stMetricLabel"]  { font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.08em; opacity: 0.55; font-weight: 500; }

/* Metric card background */
div[data-testid="metric-container"] {
    background: var(--secondary-background-color);
    border-radius: 8px;
    padding: 1rem 1.2rem;
    border: 1px solid rgba(128,128,128,0.12);
}

/* Section label */
.label {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.09em;
    font-weight: 600;
    opacity: 0.5;
    margin-bottom: 0.6rem;
}

/* Risk badge */
.badge {
    display: inline-block;
    font-size: 0.7rem;
    font-weight: 700;
    letter-spacing: 0.06em;
    padding: 2px 9px;
    border-radius: 4px;
    text-transform: uppercase;
}

/* Tab strip — remove emoji padding issues */
button[data-baseweb="tab"] { font-size: 0.82rem; font-weight: 600; letter-spacing: 0.04em; }

/* Alert banners — strip the left border default streamlit adds */
div[data-testid="stAlert"] { border-radius: 6px; }
</style>
""", unsafe_allow_html=True)

# ── Header ───────────────────────────────────────────────────────────────────
st.markdown(
    "<h2 style='margin-bottom:0;letter-spacing:-0.5px;'>FinPulse</h2>"
    "<p style='opacity:0.45;font-size:0.82rem;margin-top:2px;'>"
    "Banking Risk Observability &nbsp;·&nbsp; AWS Serverless &nbsp;·&nbsp; auto-refresh 60 s</p>",
    unsafe_allow_html=True,
)
st.divider()

tab_de, tab_biz = st.tabs(["Pipeline Health", "Risk Dashboard"])

RISK_COLORS = {
    "NORMAL":        "#16a34a",
    "SUBPRIME_RISK": "#d97706",
    "HIGH_RISK":     "#ea580c",
    "CRITICAL_RISK": "#dc2626",
}
CHART_BG   = "rgba(0,0,0,0)"
GRID_COLOR = "rgba(128,128,128,0.1)"


def _layout(**kw) -> dict:
    return dict(
        paper_bgcolor=CHART_BG,
        plot_bgcolor=CHART_BG,
        font=dict(size=11, family="system-ui, sans-serif"),
        margin=dict(t=28, b=16, l=4, r=4),
        xaxis=dict(gridcolor=GRID_COLOR, zeroline=False, linecolor=GRID_COLOR),
        yaxis=dict(gridcolor=GRID_COLOR, zeroline=False, linecolor=GRID_COLOR),
        legend=dict(orientation="h", y=-0.22, font_size=11),
        **kw,
    )


def _label(text: str) -> None:
    st.markdown(f'<div class="label">{text}</div>', unsafe_allow_html=True)


# ── PIPELINE HEALTH ──────────────────────────────────────────────────────────
with tab_de:
    @st.fragment(run_every=60)
    def de_screen():
        health    = load_pipeline_health()
        bronze    = health["bronze_total"]
        silver    = health["silver_total"]
        rejected  = bronze - silver
        pass_pct  = health["health_score"]

        # KPI strip
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Bronze — Ingested",  f"{bronze:,}")
        c2.metric("Silver — Passed DQ", f"{silver:,}")
        c3.metric("Quarantine — Rejected", f"{rejected:,}",
                  delta=f"-{rejected}" if rejected else None,
                  delta_color="inverse")
        c4.metric("DQ Pass Rate", f"{pass_pct:.1f}%")

        # Progress bar
        bar_color = "#16a34a" if pass_pct >= 95 else "#d97706" if pass_pct >= 80 else "#dc2626"
        st.markdown(f"""
        <div style="margin: 0.75rem 0 1.75rem;">
          <div style="background:rgba(128,128,128,0.12);border-radius:3px;height:6px;">
            <div style="width:{pass_pct:.1f}%;background:{bar_color};border-radius:3px;height:6px;"></div>
          </div>
          <div style="font-size:0.7rem;opacity:0.4;margin-top:5px;">{silver:,} records passed &nbsp;/&nbsp; {bronze:,} total ingested</div>
        </div>
        """, unsafe_allow_html=True)

        # Error breakdown + donut
        col_left, col_right = st.columns([1.3, 1])

        with col_left:
            _label("Rejection by Error Type")
            errors = load_error_breakdown()
            if errors:
                err_df = pd.DataFrame(errors).sort_values("count", ascending=True)
                fig = px.bar(
                    err_df, x="count", y="error_code",
                    orientation="h",
                    color="error_code",
                    color_discrete_sequence=["#dc2626", "#ea580c", "#d97706", "#6366f1", "#0ea5e9"],
                    text="count",
                )
                fig.update_traces(textposition="outside", textfont_size=10, showlegend=False)
                fig.update_layout(**_layout(height=260, xaxis_title="Records", yaxis_title=""))
                st.plotly_chart(fig, width="stretch")
            else:
                st.caption("No quarantine records — DQ gate is clean.")

        with col_right:
            _label("Pass vs Reject")
            if bronze > 0:
                fig2 = go.Figure(go.Pie(
                    labels=["Passed", "Rejected"],
                    values=[silver, rejected],
                    hole=0.68,
                    marker_colors=["#16a34a", "#dc2626"],
                    textinfo="percent",
                    hovertemplate="%{label}: %{value:,}<extra></extra>",
                ))
                fig2.add_annotation(
                    text=f"<b>{pass_pct:.1f}%</b><br><span style='font-size:10px;opacity:0.6'>pass rate</span>",
                    showarrow=False, font_size=16, align="center",
                )
                fig2.update_layout(**_layout(height=260, showlegend=True))
                st.plotly_chart(fig2, width="stretch")

        st.divider()

        # ── Quarantine Viewer ─────────────────────────────────────────────
        _label("Quarantine — Rejected Records")
        st.caption("Read-only view. No reprocessing or remediation.")

        ERROR_META = {
            "NULL_CRITICAL_FIELD":   ("#dc2626", "Missing required field — User, Card, or Amount is null"),
            "INVALID_AMOUNT":        ("#ea580c", "Amount is zero or negative"),
            "FUTURE_TIMESTAMP":      ("#d97706", "Transaction timestamp is ahead of ingestion time"),
            "DUPLICATE_TRANSACTION": ("#7c3aed", "Record already exists in Gold — dedup check failed"),
            "EXCEEDS_CREDIT_LIMIT":  ("#0284c7", "Transaction amount exceeds the card credit limit"),
        }

        # with st.expander("Error Code Reference", expanded=False):
        #     rows = []
        #     for code, (color, desc) in ERROR_META.items():
        #         rows.append(
        #             f"<tr>"
        #             f"<td style='padding:5px 12px 5px 0;'>"
        #             f"<span style='background:{color}18;border:1px solid {color}40;"
        #             f"border-radius:4px;padding:2px 8px;font-size:0.72rem;"
        #             f"font-weight:700;color:{color};font-family:monospace;'>{code}</span></td>"
        #             f"<td style='font-size:0.8rem;opacity:0.75;padding:5px 0;'>{desc}</td>"
        #             f"</tr>"
        #         )
        #     st.markdown(
        #         f"<table style='border-collapse:collapse;width:100%;'>{''.join(rows)}</table>",
        #         unsafe_allow_html=True,
        #     )

        with st.expander("Error Code Reference", expanded=False):
            rows = []
            for code, (color, desc) in ERROR_META.items():
                rows.append(
                    f"<tr>"
                    f"<td style='padding: 8px 16px 8px 4px; width: 28%; vertical-align: middle;'>"
                    f"<span style='background:{color}18;border:1px solid {color}40;"
                    f"border-radius:4px;padding:3px 10px;font-size:0.72rem;"
                    f"font-weight:700;color:{color};font-family:monospace;'>{code}</span></td>"
                    f"<td style='padding: 8px 4px 8px 12px; vertical-align: middle; font-size:0.8rem; opacity:0.75;'>{desc}</td>"
                    f"</tr>"
                )
            st.markdown(
                f"<table style='border-collapse: collapse; width: 100%;'>{''.join(rows)}</table>",
                unsafe_allow_html=True,
            )

        errors = load_error_breakdown()
        error_options = ["All"] + [e["error_code"] for e in errors]
        selected = st.selectbox(
            "Filter",
            options=error_options,
            format_func=lambda x: "All error types" if x == "All"
                else f"{x}  —  {next((e['count'] for e in errors if e['error_code']==x), 0):,} records",
            label_visibility="collapsed",
            key="q_filter",
        )

        filter_val = None if selected == "All" else selected
        records = load_quarantine_records(limit=200, filter_error=filter_val)

        if records:
            q_df = pd.DataFrame(records)
            priority = ["error_code", "error_description", "quarantine_time", "source_batch"]
            rest = [c for c in q_df.columns if c not in priority]
            q_df = q_df[priority + rest]

            st.dataframe(
                q_df,
                width="stretch",
                height=400,
                column_config={
                    "error_code": st.column_config.TextColumn(
                        "Error Code", width="medium",
                        help="DQ rule that rejected this record",
                    ),
                    "error_description": st.column_config.TextColumn(
                        "Reason", width="large",
                        help="Why this record was rejected",
                    ),
                    "quarantine_time": st.column_config.TextColumn("Rejected At", width="medium"),
                    "source_batch":    st.column_config.TextColumn("Source Batch", width="medium"),
                    "Amount":          st.column_config.NumberColumn("Amount", format="$%.2f"),
                },
            )
            suffix = f" — filtered by {selected}" if selected != "All" else ""
            st.caption(f"{len(q_df):,} records{suffix}")
        else:
            st.caption("Quarantine is empty.")

    de_screen()


# ── RISK DASHBOARD ───────────────────────────────────────────────────────────
with tab_biz:
    @st.fragment(run_every=60)
    def biz_screen():
        risk_data = load_risk_summary()
        risk_drivers = load_risk_drivers()
        risk_map  = {r["risk_label"]: r for r in risk_data} if risk_data else {}

        def _val(label, field, default=0):
            return risk_map.get(label, {}).get(field, default)

        total_txn = sum(_val(l, "count") for l in RISK_COLORS)
        total_amt = sum(_val(l, "total_amount") for l in RISK_COLORS)
        critical  = _val("CRITICAL_RISK", "count")
        high      = _val("HIGH_RISK", "count")
        subprime  = _val("SUBPRIME_RISK", "count")
        normal    = _val("NORMAL", "count")
        review_queue = int(risk_drivers.get("review_queue", critical + high) or 0)
        review_exposure = float(risk_drivers.get("review_exposure", 0) or 0)
        review_rate = review_queue / total_txn * 100 if total_txn else 0

        _label("Portfolio Risk Posture")
        st.caption(
            "A transaction enters the review queue when credit utilization (CUR) is above 85%. "
            "It becomes Critical when CUR >85% and FICO is below 620."
        )

        # KPI strip: each metric answers a decision question for the risk analyst.
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Portfolio transactions", f"{total_txn:,}",
                  help="All transactions that passed data-quality checks and reached Gold.")
        c2.metric("Needs review", f"{review_queue:,}",
                  delta=f"{review_rate:.1f}% of portfolio" if total_txn else None,
                  delta_color="inverse",
                  help="High Risk and Critical Risk transactions that should be reviewed first.")
        c3.metric("At-risk exposure", f"${review_exposure:,.0f}",
                  help="Transaction value represented by the current review queue; this is exposure, not confirmed loss.")
        c4.metric("Critical queue", f"{critical:,}",
                  delta="CUR >85% + FICO <620" if critical else None,
                  delta_color="inverse",
                  help="Highest-priority cases: highly utilized credit combined with a subprime FICO score.")
        c5.metric("Normal", f"{normal:,}",
                  delta=f"{normal / total_txn * 100:.1f}%" if total_txn else None,
                  help="Transactions with neither high utilization nor a subprime FICO signal.")

        # Status banner — plain text, no emoji
        if critical > 0:
            st.error(
                f"{critical:,} CRITICAL RISK transactions — CUR >85% combined with Subprime FICO (<620). "
                f"Immediate review recommended."
            )
        elif high > 0:
            st.warning(
                f"{high:,} HIGH RISK transactions — credit utilization exceeds 85% threshold."
            )
        else:
            st.success("No critical or high risk transactions in current dataset.")

        with st.expander("How to interpret the risk signals", expanded=True):
            guide = pd.DataFrame([
                {
                    "Signal": "High credit utilization (CUR)",
                    "Rule": "CUR > 85%",
                    "What it reflects": "The card's monthly accepted-spend proxy is close to its credit limit.",
                    "Analyst action": "Review spending pattern and available credit before approving follow-up activity.",
                },
                {
                    "Signal": "Subprime credit profile",
                    "Rule": "FICO < 620",
                    "What it reflects": "A lower creditworthiness indicator; it increases concern but is not a rejection by itself.",
                    "Analyst action": "Use as a supporting signal alongside utilization and transaction context.",
                },
                {
                    "Signal": "Critical risk",
                    "Rule": "CUR > 85% AND FICO < 620",
                    "What it reflects": "Two complementary review signals occur together, creating the highest review priority.",
                    "Analyst action": "Investigate first; validate customer, merchant, and transaction context.",
                },
                {
                    "Signal": "High velocity",
                    "Rule": "> 3 transactions per card in a micro-batch",
                    "What it reflects": "Rapid activity that can indicate unusual behavior; shown as context, not a risk label by itself.",
                    "Analyst action": "Compare with historical customer behavior and merchant pattern.",
                },
            ])
            st.dataframe(guide, width="stretch", hide_index=True, height=210)

        _label("What Is Driving the Review Queue")
        d1, d2, d3, d4 = st.columns(4)
        d1.metric("CUR above 85%", f"{int(risk_drivers.get('high_cur_count', 0) or 0):,}",
                  help="Transactions with high credit utilization. This is the direct High Risk trigger.")
        d2.metric("FICO below 620", f"{int(risk_drivers.get('subprime_count', 0) or 0):,}",
                  help="Transactions linked to a subprime customer profile.")
        d3.metric("Both signals", f"{int(risk_drivers.get('dual_factor_count', 0) or 0):,}",
                  help="Dual-factor cases; these form the Critical Risk queue.")
        d4.metric("Velocity signals", f"{int(risk_drivers.get('velocity_signal_count', 0) or 0):,}",
                  help="More than three transactions for the same card within one micro-batch. Context for investigation.")

        st.divider()

        col_l, col_r = st.columns([1, 1.45])

        with col_l:
            _label("Risk Segment Distribution")
            st.caption("Shows where the portfolio sits in the decision rules. A growing High/Critical share means the review workload is increasing.")
            if risk_data:
                risk_df = pd.DataFrame(risk_data)
                fig = px.pie(
                    risk_df, names="risk_label", values="count",
                    color="risk_label",
                    color_discrete_map=RISK_COLORS,
                    hole=0.58,
                )
                fig.update_traces(
                    textinfo="percent+label",
                    textfont_size=11,
                    hovertemplate="<b>%{label}</b><br>%{value:,} transactions  (%{percent})<extra></extra>",
                )
                fig.update_layout(**_layout(height=290, showlegend=False))
                st.plotly_chart(fig, width="stretch")

                # Summary table
                tbl = risk_df[["risk_label", "count", "total_amount"]].copy()
                tbl["Share"]  = (tbl["count"] / total_txn * 100).round(1).astype(str) + "%" if total_txn else "0%"
                tbl["Volume"] = tbl["total_amount"].apply(lambda x: f"${x:,.0f}" if x else "$0")
                tbl = tbl[["risk_label", "count", "Share", "Volume"]].rename(
                    columns={"risk_label": "Segment", "count": "Transactions"}
                )
                st.dataframe(tbl, width="stretch", hide_index=True, height=175)

        with col_r:
            _label("Daily Transaction Volume & Risk Count")
            st.caption("Bars show pipeline volume. The red line shows review-queue arrivals, helping separate higher risk from higher traffic.")
            trend_data = load_daily_trend()
            if trend_data:
                trend_df = pd.DataFrame(trend_data)
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=trend_df["day"], y=trend_df["txn_count"],
                    name="Total", marker_color="#6366f1", opacity=0.65,
                    hovertemplate="<b>%{x}</b><br>Total: %{y:,}<extra></extra>",
                ))
                fig.add_trace(go.Scatter(
                    x=trend_df["day"], y=trend_df["risk_count"],
                    name="High / Critical Risk",
                    mode="lines+markers",
                    line=dict(color="#dc2626", width=1.8),
                    marker=dict(size=4),
                    yaxis="y2",
                    hovertemplate="<b>%{x}</b><br>Risk: %{y:,}<extra></extra>",
                ))
                fig.update_layout(**_layout(height=290))
                fig.update_yaxes(title_text="Transactions")
                fig.update_layout(
                    yaxis2=dict(
                        title="Risk Count",
                        overlaying="y",
                        side="right",
                        gridcolor=GRID_COLOR,
                        zeroline=False,
                    )
                )
                st.plotly_chart(fig, width="stretch")
            else:
                st.caption("No Gold data yet.")

        st.divider()

        col_m1, col_m2 = st.columns(2)

        with col_m1:
            _label("MCC Category — Volume & Avg Credit Utilization")
            st.caption("Prioritize merchant categories with both meaningful spend and warm/red CUR coloring; they concentrate utilization pressure.")
            mcc_data = load_mcc_breakdown()
            if mcc_data:
                mcc_df = pd.DataFrame(mcc_data).head(15)
                fig = px.bar(
                    mcc_df.sort_values("total_amount", ascending=True),
                    x="total_amount", y="MCC",
                    orientation="h",
                    color="avg_cur",
                    color_continuous_scale=["#16a34a", "#d97706", "#dc2626"],
                    color_continuous_midpoint=0.5,
                    labels={"total_amount": "Total Amount ($)", "avg_cur": "Avg CUR"},
                    text="txn_count",
                )
                fig.update_traces(
                    texttemplate="%{text:,}",
                    textposition="outside",
                    textfont_size=9,
                )
                fig.update_layout(
                    **_layout(height=380),
                    coloraxis_colorbar=dict(title="Avg CUR", len=0.55, thickness=10),
                )
                st.plotly_chart(fig, width="stretch")

        with col_m2:
            _label("Analyst Review Queue — Latest High/Critical Transactions")
            st.caption("Start here: transactions are ordered by ingestion time. CUR and Subprime explain why each case was surfaced.")
            hr_records = load_recent_high_risk(limit=50)
            if hr_records:
                hr_df = pd.DataFrame(hr_records)
                if "Amount" in hr_df.columns:
                    hr_df["Amount"] = hr_df["Amount"].apply(lambda x: f"${x:,.2f}" if x else "$0")
                if "cur_ratio" in hr_df.columns:
                    hr_df["CUR"] = (hr_df["cur_ratio"].astype(float) * 100).round(1).astype(str) + "%"
                    hr_df = hr_df.drop(columns=["cur_ratio"])

                show = [c for c in [
                    "transaction_timestamp", "Amount", "Merchant Name",
                    "MCC", "risk_label", "CUR", "flag_subprime", "flag_high_velocity",
                ] if c in hr_df.columns]

                st.dataframe(
                    hr_df[show],
                    width="stretch",
                    height=380,
                    column_config={
                        "transaction_timestamp": st.column_config.TextColumn("Timestamp", width="medium"),
                        "risk_label":   st.column_config.TextColumn("Risk",    width="small"),
                        "Amount":       st.column_config.TextColumn("Amount",  width="small"),
                        "CUR":          st.column_config.TextColumn("CUR",     width="small"),
                        "flag_subprime": st.column_config.CheckboxColumn("Subprime"),
                        "flag_high_velocity": st.column_config.CheckboxColumn("High velocity"),
                    },
                )
                st.caption(f"{len(hr_df):,} records — latest by ingestion time")
            else:
                st.caption("No high-risk transactions.")

    biz_screen()
