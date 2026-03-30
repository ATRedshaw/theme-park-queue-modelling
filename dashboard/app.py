"""Streamlit dashboard for exploring theme park crowd level predictions."""

import os
import sys
from typing import Optional

# Resolve project root and point Python at the crowd-level model package
# before any local imports are attempted. All relative file paths in the
# pipeline (config.yml, data/queue_Data.db) are anchored to the project root.
_dashboard_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.abspath(os.path.join(_dashboard_dir, ".."))
_model_dir = os.path.join(_project_root, "models", "crowd-level")

os.chdir(_project_root)

if _model_dir not in sys.path:
    sys.path.insert(0, _model_dir)

import joblib
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import yaml

from utils.pipeline import model_pipeline

# ─── Page config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Crowd Level Explorer",
    page_icon="🎢",
    layout="wide",
)

# ─── Helpers ──────────────────────────────────────────────────────────────────


@st.cache_resource(show_spinner="Loading model…")
def load_model_and_columns() -> tuple:
    """
    Load the trained RandomForest model and feature columns from disk.

    Returns:
        Tuple of (model, feature_columns, error_message). On success,
        error_message is None. On failure, model and feature_columns are None.
    """
    with open("config.yml") as f:
        config = yaml.safe_load(f)

    model_name = (
        config.get("models", {})
        .get("crowd-level", {})
        .get("inference", {})
        .get("model_name", "crowd-level-model")
    )
    model_path = os.path.join(_model_dir, "model-exports", f"{model_name}.pkl")
    columns_path = os.path.join(_model_dir, "model-exports", f"{model_name}_columns.pkl")

    if not os.path.exists(model_path):
        return None, None, f"Model file not found: {model_path}"
    if not os.path.exists(columns_path):
        return None, None, f"Columns file not found: {columns_path}"

    model = joblib.load(model_path)
    feature_columns = joblib.load(columns_path)
    return model, feature_columns, None


@st.cache_data(show_spinner=False)
def fetch_park_names(park_ids: tuple) -> dict:
    """
    Fetch human-readable park names from the Queue Times API.

    Falls back to 'Park {id}' if the API is unreachable or the ID is unknown.

    Args:
        park_ids: Tuple of integer park IDs (tuple so it's hashable for caching).

    Returns:
        Dict mapping park_id (int) to park name (str).
    """
    try:
        from utils.helpers import get_name_from_queuetimes_id

        return {
            pid: (get_name_from_queuetimes_id(pid) or f"Park {pid}")
            for pid in park_ids
        }
    except Exception:
        return {pid: f"Park {pid}" for pid in park_ids}


@st.cache_data(show_spinner="Running inference pipeline — this may take a minute on first run…")
def run_inference(park_id: int, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """
    Run the full crowd-level inference pipeline for a park and date range.

    Predictions include a ±1σ confidence interval derived from the variance
    across individual RandomForest trees.

    Args:
        park_id: Queue Times park ID.
        start_date: ISO date string (YYYY-MM-DD) for the range start.
        end_date: ISO date string (YYYY-MM-DD) for the range end.

    Returns:
        DataFrame with columns: date, crowd_level, ci_lower, ci_upper.
        Returns None if the model could not be loaded.
    """
    model, feature_columns, err = load_model_and_columns()
    if err:
        return None

    dates = pd.date_range(start=start_date, end=end_date)
    day_df = pd.DataFrame({"date": dates, "park_id": str(park_id)})

    processed = model_pipeline(is_training=False, day_df=day_df)
    meta_df = processed[["date"]].copy()

    X = processed.reindex(columns=feature_columns, fill_value=0)

    # Collect per-tree predictions to produce a rough uncertainty estimate.
    tree_preds = np.array([tree.predict(X) for tree in model.estimators_])
    mean_pred = tree_preds.mean(axis=0)
    std_pred = tree_preds.std(axis=0)

    return pd.DataFrame(
        {
            "date": meta_df["date"].values,
            "crowd_level": mean_pred.round().astype(int),
            "ci_lower": np.clip(mean_pred - std_pred, 0, 100).round().astype(int),
            "ci_upper": np.clip(mean_pred + std_pred, 0, 100).round().astype(int),
        }
    )


def build_importance_df(model, feature_columns: list, top_n: int) -> pd.DataFrame:
    """
    Return a sorted feature importance DataFrame.

    Args:
        model: Trained RandomForest model with feature_importances_ attribute.
        feature_columns: Ordered list of feature names matching model training.
        top_n: Number of top features to return.

    Returns:
        DataFrame with 'feature' and 'importance' columns, sorted ascending
        (so a horizontal bar chart renders highest importance at the top).
    """
    df = pd.DataFrame(
        {"feature": feature_columns, "importance": model.feature_importances_}
    )
    return df.sort_values("importance", ascending=False).head(top_n).sort_values("importance")


def _label_crowd_level(val: int) -> str:
    """Map a 0–100 percentile crowd score to a descriptive label."""
    if val < 25:
        return "Quiet"
    if val < 50:
        return "Moderate"
    if val < 75:
        return "Busy"
    return "Very Busy"


# ─── Sidebar ──────────────────────────────────────────────────────────────────

with open("config.yml") as f:
    _config = yaml.safe_load(f)

_park_ids: list = _config.get("scraper", {}).get("park_ids", [2])

with st.sidebar:
    st.title("🎢 Crowd Level Explorer")
    st.divider()

    park_names = fetch_park_names(tuple(_park_ids))
    park_options = {f"{park_names[pid]} (ID {pid})": pid for pid in _park_ids}

    selected_label = st.selectbox("Park", options=list(park_options.keys()))
    selected_park_id: int = park_options[selected_label]

    today = pd.Timestamp.today().normalize()
    start_date = st.date_input("Start date", value=today.date())
    end_date = st.date_input("End date", value=(today + pd.Timedelta(days=30)).date())

    st.divider()
    run_button = st.button("Run Predictions", type="primary", use_container_width=True)
    st.caption("Results are cached — re-running the same park/dates is instant.")

# ─── Session state initialisation ─────────────────────────────────────────────

if "results" not in st.session_state:
    st.session_state["results"] = None
    st.session_state["run_park_id"] = None

# ─── Run inference on button press ────────────────────────────────────────────

if run_button:
    if start_date > end_date:
        st.sidebar.error("Start date must be before end date.")
        st.stop()

    results = run_inference(
        park_id=selected_park_id,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
    )

    if results is None:
        _, _, err = load_model_and_columns()
        st.error(f"Could not load the model: {err}")
        st.stop()

    if results.empty:
        st.warning(
            "No predictions returned — the park may not have opening hours data "
            "for the selected date range."
        )
        st.stop()

    st.session_state["results"] = results
    st.session_state["run_park_id"] = selected_park_id

# ─── Main content ─────────────────────────────────────────────────────────────

results: Optional[pd.DataFrame] = st.session_state["results"]
run_park_id: Optional[int] = st.session_state["run_park_id"]

if results is None:
    # No predictions yet — show feature importance as a teaser and instructions.
    st.title("Theme Park Crowd Level Explorer")
    st.markdown(
        "Select a park and date range in the sidebar, then hit **Run Predictions** "
        "to see crowd level forecasts, confidence intervals, and feature importance."
    )

    model, feature_columns, err = load_model_and_columns()
    if model is not None:
        st.subheader("Feature Importance — trained model")
        top_n_default = min(20, len(feature_columns))
        importance_df = build_importance_df(model, feature_columns, top_n=top_n_default)
        fig_imp = px.bar(
            importance_df,
            x="importance",
            y="feature",
            orientation="h",
            color="importance",
            color_continuous_scale="Blues",
            labels={"importance": "Importance", "feature": "Feature"},
        )
        fig_imp.update_layout(
            coloraxis_showscale=False,
            height=max(400, top_n_default * 26),
            yaxis_title=None,
            margin=dict(l=10, r=10, t=10, b=10),
        )
        st.plotly_chart(fig_imp, use_container_width=True)
    elif err:
        st.error(err)

else:
    park_label = park_names.get(run_park_id, f"Park {run_park_id}")
    st.title(f"🎢 {park_label}")

    # ── Summary metrics ────────────────────────────────────────────────────────
    mean_level = int(results["crowd_level"].mean())
    peak_level = int(results["crowd_level"].max())
    quietest = results.loc[results["crowd_level"].idxmin(), "date"]
    busiest = results.loc[results["crowd_level"].idxmax(), "date"]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Mean Crowd Level", f"{mean_level} / 100", help="Average predicted crowd percentile across the date range.")
    col2.metric("Peak", f"{peak_level} / 100", delta=f"{peak_level - mean_level:+d} vs mean", delta_color="inverse")
    col3.metric("Quietest Day", pd.Timestamp(quietest).strftime("%a %d %b"))
    col4.metric("Busiest Day", pd.Timestamp(busiest).strftime("%a %d %b"))

    st.divider()

    # ── Tabs ──────────────────────────────────────────────────────────────────
    tab_predictions, tab_importance, tab_data = st.tabs(
        ["📈 Predictions", "🔍 Feature Importance", "📋 Raw Data"]
    )

    with tab_predictions:
        fig = go.Figure()

        # CI band (±1σ across RF trees)
        ci_x = list(results["date"]) + list(results["date"])[::-1]
        ci_y = list(results["ci_upper"]) + list(results["ci_lower"])[::-1]
        fig.add_trace(
            go.Scatter(
                x=ci_x,
                y=ci_y,
                fill="toself",
                fillcolor="rgba(99, 110, 250, 0.15)",
                line=dict(color="rgba(0,0,0,0)"),
                hoverinfo="skip",
                name="±1σ range",
                showlegend=True,
            )
        )

        # Main prediction line with colour-coded markers
        fig.add_trace(
            go.Scatter(
                x=results["date"],
                y=results["crowd_level"],
                mode="lines+markers",
                name="Crowd Level",
                line=dict(color="#636EFA", width=2),
                marker=dict(
                    size=7,
                    color=results["crowd_level"],
                    colorscale="RdYlGn_r",
                    cmin=0,
                    cmax=100,
                    showscale=True,
                    colorbar=dict(title="Crowd<br>Level", thickness=14),
                ),
                hovertemplate=(
                    "<b>%{x|%A, %d %b %Y}</b><br>"
                    "Crowd Level: %{y}<br>"
                    "<extra></extra>"
                ),
            )
        )

        # Reference bands for quiet / busy thresholds
        for threshold, label, colour in [
            (25, "Quiet", "rgba(50,200,80,0.07)"),
            (50, "Moderate", "rgba(240,180,0,0.07)"),
            (75, "Busy", "rgba(240,80,50,0.07)"),
        ]:
            fig.add_hrect(
                y0=0 if threshold == 25 else threshold - 25,
                y1=threshold,
                fillcolor=colour,
                line_width=0,
                annotation_text=label,
                annotation_position="left",
                annotation_font_size=11,
            )

        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Crowd Level (0 = quietest, 100 = busiest)",
            yaxis=dict(range=[0, 108]),
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            height=480,
            margin=dict(l=60, r=20, t=30, b=60),
        )
        st.plotly_chart(fig, use_container_width=True)

        st.caption(
            "Crowd level is a percentile rank (0–100) of average queue time for that park. "
            "Shaded band shows ±1 standard deviation across Random Forest trees."
        )

    with tab_importance:
        model, feature_columns, err = load_model_and_columns()
        if model is not None:
            top_n = st.slider(
                "Top N features",
                min_value=5,
                max_value=min(len(feature_columns), 40),
                value=min(20, len(feature_columns)),
                step=5,
            )
            importance_df = build_importance_df(model, feature_columns, top_n=top_n)

            fig_imp = px.bar(
                importance_df,
                x="importance",
                y="feature",
                orientation="h",
                color="importance",
                color_continuous_scale="Blues",
                labels={"importance": "Importance", "feature": "Feature"},
            )
            fig_imp.update_layout(
                coloraxis_showscale=False,
                height=max(420, top_n * 26),
                yaxis_title=None,
                xaxis_title="Importance (mean decrease in impurity)",
                margin=dict(l=10, r=20, t=20, b=40),
            )
            st.plotly_chart(fig_imp, use_container_width=True)
        elif err:
            st.error(err)

    with tab_data:
        display = results.copy()
        display["day"] = pd.to_datetime(display["date"]).dt.strftime("%A")
        display["busy_label"] = display["crowd_level"].apply(_label_crowd_level)
        display["date"] = pd.to_datetime(display["date"]).dt.strftime("%Y-%m-%d")
        display = display[["date", "day", "crowd_level", "ci_lower", "ci_upper", "busy_label"]]
        display.columns = ["Date", "Day", "Crowd Level", "CI Lower (−1σ)", "CI Upper (+1σ)", "Category"]

        st.dataframe(
            display,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Crowd Level": st.column_config.ProgressColumn(
                    "Crowd Level",
                    min_value=0,
                    max_value=100,
                    format="%d",
                ),
            },
        )

        csv = display.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇ Download CSV",
            data=csv,
            file_name=f"crowd-predictions-park-{run_park_id}.csv",
            mime="text/csv",
        )
