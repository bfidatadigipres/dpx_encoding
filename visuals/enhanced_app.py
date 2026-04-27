import os
import sqlite3
import sys

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st

RESOLUTION_ORDER = ["Up to 2K (2048)", "Over 2K / Up to 4K (4096)", "Over 4K (4097+)", "Unknown"]

def get_data(database):
    """
    Fetch file-size, metadata, and encoding timestamps
    from the encoding database.

    *** Adjust column names if yours differ. ***
    """
    conn = sqlite3.connect(database)
    query = """
        SELECT seq_id,
               seq_size,
               derivative_size,
               colourspace,
               image_width,
               image_height,
               bitdepth,
               assessment_complete,
               encoding_complete
        FROM encoding_status
        WHERE status LIKE '%MKV validation complete'
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def calculate_savings(df):
    """Absolute and percentage savings."""
    df["savings"] = df["seq_size"] - df["derivative_size"]
    df["savings_pct"] = (
        (df["savings"] / df["seq_size"]) * 100
    ).round(2)
    return df


def calculate_encoding_times(df):
    """
    Parse the start / end timestamps and derive
    encoding duration in minutes and hours.
    """
    df["encoding_start"] = pd.to_datetime(
        df["assessment_complete"], format="%Y-%m-%d %H:%M:%S", errors="coerce"
    )
    df["encoding_end"] = pd.to_datetime(
        df["encoding_complete"], format="%Y-%m-%d %H:%M:%S", errors="coerce"
    )
    df["encoding_duration"] = df["encoding_end"] - df["encoding_start"]
    df["encoding_minutes"] = (
        df["encoding_duration"].dt.total_seconds() / 60
    ).round(2)
    df["encoding_hours"] = (
        df["encoding_duration"].dt.total_seconds() / 3600
    ).round(4)
    return df


def categorize_resolution(width):
    """Bucket image width into resolution groups."""
    if pd.isna(width):
        return "Unknown"
    width = int(width)
    if width <= 2048:
        return "Up to 2K (2048)"
    elif width <= 4096:
        return "Over 2K / Up to 4K (4096)"
    return "Over 4K (4097+)"


def categorize_bitdepth_colourspace(row):
    """
    Combine bit depth and colourspace into a single label.
    Colourspace values are FFprobe pixel format strings,
    grouped by their prefix.
    """
    bitdepth = row.get("bitdepth")
    colourspace = row.get("colourspace")

    if pd.isna(bitdepth):
        return "Unknown"
    bitdepth = int(bitdepth)

    if pd.isna(colourspace) or colourspace is None:
        cs_label = "Unknown"
    else:
        cs = str(colourspace).strip().lower()
        if cs.startswith("gray") or cs.startswith("Y"):
            cs_label = "Luma"
        elif cs.startswith("rgb") or cs.startswith("gbrp"):
            cs_label = "RGB"
        elif cs.startswith("bgr"):
            cs_label = "BGR"
        else:
            cs_label = colourspace.strip()

    return f"{bitdepth}-bit {cs_label}"


def format_size(n_bytes):
    """Human-readable byte sizes."""
    for unit in ("bytes", "KB", "MB", "GB", "TB"):
        if abs(n_bytes) < 1024:
            return f"{n_bytes:.2f} {unit}"
        n_bytes /= 1024
    return f"{n_bytes:.2f} PB"


def format_duration(minutes):
    """Human-readable duration from minutes."""
    if pd.isna(minutes):
        return "N/A"
    if minutes < 60:
        return f"{minutes:.1f} min"
    hours = minutes / 60
    if hours < 24:
        return f"{hours:.1f} hrs"
    days = hours / 24
    return f"{days:.1f} days"


def make_summary(df, cat_col):
    """Return one row per category with totals and averages."""
    summary = (
        df.groupby(cat_col)
        .agg(
            total_before=("seq_size", "sum"),
            total_after=("derivative_size", "sum"),
            total_savings=("savings", "sum"),
            avg_savings_pct=("savings_pct", "mean"),
            count=("seq_id", "count"),
        )
        .reset_index()
    )
    summary["overall_savings_pct"] = (
        (summary["total_savings"] / summary["total_before"]) * 100
    ).round(2)
    return summary


def make_time_summary(df, cat_col):
    """
    Return one row per category with encoding time stats.
    Only considers rows that have valid encoding times.
    """
    valid = df.dropna(subset=["encoding_minutes"])
    if valid.empty:
        return pd.DataFrame()
    summary = (
        valid.groupby(cat_col)
        .agg(
            avg_minutes=("encoding_minutes", "mean"),
            median_minutes=("encoding_minutes", "median"),
            min_minutes=("encoding_minutes", "min"),
            max_minutes=("encoding_minutes", "max"),
            total_minutes=("encoding_minutes", "sum"),
            count=("seq_id", "count"),
        )
        .reset_index()
    )
    summary["avg_minutes"] = summary["avg_minutes"].round(2)
    summary["median_minutes"] = summary["median_minutes"].round(2)
    return summary


def build_category_bar(summary, cat_col, title):
    """Grouped bar: before / after / savings in TB per category."""
    s = summary.copy()
    s["before_tb"]  = s["total_before"]  / (1024 ** 4)
    s["after_tb"]   = s["total_after"]   / (1024 ** 4)
    s["savings_tb"] = s["total_savings"] / (1024 ** 4)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=s[cat_col], y=s["before_tb"],
        name="Before (TB)", marker_color="#3b82f6",
        text=s["before_tb"].round(2), textposition="outside",
    ))
    fig.add_trace(go.Bar(
        x=s[cat_col], y=s["after_tb"],
        name="After (TB)", marker_color="#eab308",
        text=s["after_tb"].round(2), textposition="outside",
    ))
    fig.add_trace(go.Bar(
        x=s[cat_col], y=s["savings_tb"],
        name="Savings (TB)", marker_color="#ef4444",
        text=s["savings_tb"].round(2), textposition="outside",
    ))
    fig.update_layout(
        title=title, xaxis_title="Category", yaxis_title="Size (TB)",
        barmode="group", hovermode="x unified", showlegend=True,
    )
    return fig


def build_pct_bar(summary, cat_col, title):
    """Bar chart of average percentage savings per category."""
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=summary[cat_col], y=summary["avg_savings_pct"],
        marker_color="#22c55e",
        text=summary["avg_savings_pct"].round(1).astype(str) + " %",
        textposition="outside",
    ))
    fig.update_layout(
        title=title, xaxis_title="Category",
        yaxis_title="Average Savings (%)", showlegend=False,
        yaxis_range=[0, max(100, summary["avg_savings_pct"].max() + 10)],
    )
    return fig


def build_count_pie(summary, cat_col, title):
    """Donut chart showing sequence count per category."""
    fig = go.Figure(data=[go.Pie(
        labels=summary[cat_col], values=summary["count"],
        hole=0.4, textinfo="label+value+percent",
    )])
    fig.update_layout(title_text=title, showlegend=True)
    return fig


def build_detail_bar(sub_df):
    """Per-sequence bar chart for a filtered subset."""
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=sub_df["seq_id"], y=sub_df["seq_size"],
        name="Before", marker_color="#3b82f6",
    ))
    fig.add_trace(go.Bar(
        x=sub_df["seq_id"], y=sub_df["derivative_size"],
        name="After", marker_color="#eab308",
    ))
    fig.add_trace(go.Bar(
        x=sub_df["seq_id"], y=sub_df["savings"],
        name="Savings", marker_color="#ef4444",
    ))
    fig.update_layout(
        barmode="group", hovermode="x unified", height=360,
        xaxis_title="Sequence", yaxis_title="Size (bytes)",
    )
    return fig


def build_avg_time_bar(time_summary, cat_col, title):
    """Grouped bar: average and median encoding time per category."""
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=time_summary[cat_col],
        y=time_summary["avg_minutes"],
        name="Average (min)",
        marker_color="#8b5cf6",
        text=time_summary["avg_minutes"].round(1),
        textposition="outside",
    ))
    fig.add_trace(go.Bar(
        x=time_summary[cat_col],
        y=time_summary["median_minutes"],
        name="Median (min)",
        marker_color="#06b6d4",
        text=time_summary["median_minutes"].round(1),
        textposition="outside",
    ))
    fig.update_layout(
        title=title, xaxis_title="Category",
        yaxis_title="Encoding Time (minutes)",
        barmode="group", hovermode="x unified", showlegend=True,
    )
    return fig


def build_time_range_bar(time_summary, cat_col, title):
    """Bar with min/max range and average marker per category."""
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=time_summary[cat_col],
        y=time_summary["max_minutes"],
        name="Max (min)",
        marker_color="rgba(239, 68, 68, 0.4)",
        text=time_summary["max_minutes"].round(1),
        textposition="outside",
    ))
    fig.add_trace(go.Bar(
        x=time_summary[cat_col],
        y=time_summary["avg_minutes"],
        name="Average (min)",
        marker_color="#8b5cf6",
        text=time_summary["avg_minutes"].round(1),
        textposition="outside",
    ))
    fig.add_trace(go.Bar(
        x=time_summary[cat_col],
        y=time_summary["min_minutes"],
        name="Min (min)",
        marker_color="rgba(34, 197, 94, 0.6)",
        text=time_summary["min_minutes"].round(1),
        textposition="outside",
    ))
    fig.update_layout(
        title=title, xaxis_title="Category",
        yaxis_title="Encoding Time (minutes)",
        barmode="group", hovermode="x unified", showlegend=True,
    )
    return fig


def build_time_vs_size_scatter(df, cat_col):
    """Scatter plot: file size vs encoding time, coloured by category."""
    valid = df.dropna(subset=["encoding_minutes"]).copy()
    if valid.empty:
        return None
    valid["seq_size_gb"] = valid["seq_size"] / (1024 ** 3)
    fig = px.scatter(
        valid,
        x="seq_size_gb",
        y="encoding_minutes",
        color=cat_col,
        hover_data=["seq_id", "image_width", "image_height", "bitdepth"],
        title="Encoding Time vs Source File Size",
        labels={
            "seq_size_gb": "Source Size (GB)",
            "encoding_minutes": "Encoding Time (min)",
            cat_col: "Category",
        },
    )
    fig.update_layout(hovermode="closest")
    return fig


def build_time_box(df, cat_col, title):
    """Box plot of encoding times per category."""
    valid = df.dropna(subset=["encoding_minutes"])
    if valid.empty:
        return None
    fig = px.box(
        valid,
        x=cat_col,
        y="encoding_minutes",
        color=cat_col,
        title=title,
        labels={
            "encoding_minutes": "Encoding Time (min)",
            cat_col: "Category",
        },
        points="outliers",
    )
    fig.update_layout(showlegend=False)
    return fig


def render_category_detail(df, summary, cat_col, time_summary=None, sort_order=None):
    """Expanders with per-category metrics, bar chart, and encoding times."""
    cats = summary[cat_col].tolist()
    if sort_order:
        cats = [c for c in sort_order if c in cats] + [
            c for c in cats if c not in sort_order
        ]
    for idx, cat in enumerate(cats):
        with st.expander(f"🔍  {cat}"):
            sub = df[df[cat_col] == cat]
            row = summary[summary[cat_col] == cat].iloc[0]

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Sequences", int(row["count"]))
            c2.metric("Total Before", format_size(row["total_before"]))
            c3.metric("Total Savings", format_size(row["total_savings"]))
            c4.metric("Avg Savings %", f"{row['avg_savings_pct']:.1f} %")

            # Encoding time metrics if available
            if time_summary is not None and not time_summary.empty:
                time_row = time_summary[time_summary[cat_col] == cat]
                if not time_row.empty:
                    tr = time_row.iloc[0]
                    t1, t2, t3, t4 = st.columns(4)
                    t1.metric(
                        "Avg Encoding",
                        format_duration(tr["avg_minutes"]),
                    )
                    t2.metric(
                        "Median Encoding",
                        format_duration(tr["median_minutes"]),
                    )
                    t3.metric(
                        "Fastest",
                        format_duration(tr["min_minutes"]),
                    )
                    t4.metric(
                        "Slowest",
                        format_duration(tr["max_minutes"]),
                    )

            st.plotly_chart(
                build_detail_bar(sub),
                width='stretch',
                key=f"detail_bar_{cat_col}_{idx}",
            )


def main():
    st.set_page_config(
        page_title="RAWcooked Compression Dashboard",
        layout="wide",
    )
    st.title("RAWcooked Compression Storage Savings Dashboard")

    database = os.environ.get("DATABASE", None)
    if database is None:
        sys.exit("Database could not be found.")

    # ── Load / refresh button ──────────────────────────
    if st.button("Load / Refresh Data"):
        try:
            df = get_data(database)
            df = calculate_savings(df)
            df = calculate_encoding_times(df)
            df["resolution_group"] = df["image_width"].apply(categorize_resolution)
            df["bitdepth_cs"] = df.apply(
                categorize_bitdepth_colourspace, axis=1
            )
            st.session_state["df"] = df
        except Exception as err:
            st.error(f"Error loading data: {err}")
            return

    if "df" not in st.session_state:
        st.info("Click **Load / Refresh Data** to begin.")
        return

    df = st.session_state["df"]

    # ── Sidebar filters ───────────────────────────────
    with st.sidebar:
        st.header("Filters")

        all_bd = sorted(df["bitdepth_cs"].unique())
        sel_bd = st.multiselect(
            "Bit Depth / Colourspace", all_bd, default=all_bd
        )

        all_res = [r for r in RESOLUTION_ORDER if r in df["resolution_group"].unique()]
        sel_res = st.multiselect(
            "Resolution Group", all_res, default=all_res
        )

    filtered = df[
        df["bitdepth_cs"].isin(sel_bd) & df["resolution_group"].isin(sel_res)
    ]

    if filtered.empty:
        st.warning("No data matches the current filter selection.")
        return

    # ── Top-level metrics ─────────────────────────────
    total_before = filtered["seq_size"].sum()
    total_after  = filtered["derivative_size"].sum()
    total_saved  = filtered["savings"].sum()
    avg_pct      = filtered["savings_pct"].mean()
    total_files  = filtered["seq_id"].nunique()
    valid_times  = filtered["encoding_minutes"].dropna()
    avg_enc_time = valid_times.mean() if not valid_times.empty else None

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Sequences", f"{total_files:,}")
    m2.metric("Total Before", format_size(total_before))
    m3.metric("Total After", format_size(total_after))
    m4.metric("Total Saved", format_size(total_saved))
    m5.metric("Avg Saving", f"{avg_pct:.1f} %")
    m6.metric(
        "Avg Encode Time",
        format_duration(avg_enc_time) if avg_enc_time else "N/A",
    )

    # ── Tabs ──────────────────────────────────────────
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Overall Savings",
        "🎨 Bit Depth & Colourspace",
        "📐 Resolution Group",
        "⏱️ Encoding Times",
        "📋 Data Table",
    ])

    # ── TAB 1: Overall ────────────────────────────────
    with tab1:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=filtered["seq_id"], y=filtered["seq_size"],
            name="Before (bytes)", marker_color="#3b82f6",
            text=filtered["seq_size"], textposition="outside",
        ))
        fig.add_trace(go.Bar(
            x=filtered["seq_id"], y=filtered["derivative_size"],
            name="After (bytes)", marker_color="#eab308",
            text=filtered["derivative_size"], textposition="outside",
        ))
        fig.add_trace(go.Bar(
            x=filtered["seq_id"], y=filtered["savings"],
            name="Savings (bytes)", marker_color="#ef4444",
            text=filtered["savings"], textposition="outside",
        ))
        fig.update_layout(
            title="Per-sequence File Sizes and Savings",
            xaxis_title="Sequence", yaxis_title="Size (bytes)",
            barmode="group", hovermode="x unified", showlegend=True,
        )
        st.plotly_chart(fig, width='stretch')

        pie = go.Figure(data=[go.Pie(
            labels=["Before (TB)", "After (TB)", "Savings (TB)"],
            values=[
                total_before / 1024 ** 4,
                total_after  / 1024 ** 4,
                total_saved  / 1024 ** 4,
            ],
            hole=0.4,
            marker=dict(colors=["#3b82f6", "#eab308", "#ef4444"]),
            textinfo="label+percent",
        )])
        pie.update_layout(title_text="Aggregate (TB)", showlegend=True)
        st.plotly_chart(pie, width='stretch')
        st.markdown(f"**Total sequences RAWcooked:** {total_files}")

    # ── TAB 2: Bit Depth & Colourspace ────────────────
    with tab2:
        st.subheader("Savings by Bit Depth & Colourspace")
        summary_bd = make_summary(filtered, "bitdepth_cs")
        summary_bd = summary_bd.sort_values("bitdepth_cs").reset_index(drop=True)
        time_summary_bd = make_time_summary(filtered, "bitdepth_cs")

        col_a, col_b = st.columns(2)
        with col_a:
            st.plotly_chart(
                build_category_bar(
                    summary_bd, "bitdepth_cs",
                    "Total Sizes by Bit Depth & Colourspace",
                ),
                width='stretch',
            )
        with col_b:
            st.plotly_chart(
                build_pct_bar(
                    summary_bd, "bitdepth_cs",
                    "Average % Savings by Bit Depth & Colourspace",
                ),
                width='stretch',
            )

        # Encoding time charts within this tab
        if not time_summary_bd.empty:
            st.subheader("Encoding Times by Bit Depth & Colourspace")
            col_c, col_d = st.columns(2)
            with col_c:
                st.plotly_chart(
                    build_avg_time_bar(
                        time_summary_bd, "bitdepth_cs",
                        "Avg / Median Encoding Time",
                    ),
                    width='stretch',
                )
            with col_d:
                st.plotly_chart(
                    build_time_range_bar(
                        time_summary_bd, "bitdepth_cs",
                        "Min / Avg / Max Encoding Time",
                    ),
                    width='stretch',
                )

        st.plotly_chart(
            build_count_pie(
                summary_bd, "bitdepth_cs",
                "Sequence Count by Bit Depth & Colourspace",
            ),
            width='stretch',
        )

        st.subheader("Category Detail")
        render_category_detail(
            filtered, summary_bd, "bitdepth_cs",
            time_summary=time_summary_bd,
        )

    # ── TAB 3: Resolution Group ───────────────────────
    with tab3:
        st.subheader("Savings by Resolution Group")
        summary_res = make_summary(filtered, "resolution_group")
        order_map = {v: i for i, v in enumerate(RESOLUTION_ORDER)}
        summary_res["_sort"] = summary_res["resolution_group"].map(order_map)
        summary_res = (
            summary_res.sort_values("_sort")
            .drop(columns="_sort")
            .reset_index(drop=True)
        )
        time_summary_res = make_time_summary(filtered, "resolution_group")

        col_a, col_b = st.columns(2)
        with col_a:
            st.plotly_chart(
                build_category_bar(
                    summary_res, "resolution_group",
                    "Total Sizes by Resolution Group",
                ),
                width='stretch',
            )
        with col_b:
            st.plotly_chart(
                build_pct_bar(
                    summary_res, "resolution_group",
                    "Average % Savings by Resolution Group",
                ),
                width='stretch',
            )

        if not time_summary_res.empty:
            st.subheader("Encoding Times by Resolution Group")
            col_c, col_d = st.columns(2)
            with col_c:
                st.plotly_chart(
                    build_avg_time_bar(
                        time_summary_res, "resolution_group",
                        "Avg / Median Encoding Time",
                    ),
                    width='stretch',
                )
            with col_d:
                st.plotly_chart(
                    build_time_range_bar(
                        time_summary_res, "resolution_group",
                        "Min / Avg / Max Encoding Time",
                    ),
                    width='stretch',
                )

        st.plotly_chart(
            build_count_pie(
                summary_res, "resolution_group",
                "Sequence Count by Resolution Group",
            ),
            width='stretch',
        )

        st.subheader("Category Detail")
        render_category_detail(
            filtered, summary_res, "resolution_group",
            time_summary=time_summary_res,
            sort_order=RESOLUTION_ORDER,
        )

    # ── TAB 4: Encoding Times ─────────────────────────
    with tab4:
        st.subheader("Encoding Time Analysis")

        valid_df = filtered.dropna(subset=["encoding_minutes"])
        if valid_df.empty:
            st.warning(
                "No valid encoding timestamps found in the current selection."
            )
        else:
            # Top-level time metrics
            et1, et2, et3, et4 = st.columns(4)
            et1.metric("Avg Encoding", format_duration(valid_df["encoding_minutes"].mean()))
            et2.metric("Median Encoding", format_duration(valid_df["encoding_minutes"].median()))
            et3.metric("Fastest", format_duration(valid_df["encoding_minutes"].min()))
            et4.metric("Slowest", format_duration(valid_df["encoding_minutes"].max()))

            # Box plot by bitdepth/colourspace
            box_fig = build_time_box(
                filtered, "bitdepth_cs",
                "Encoding Time Distribution by Bit Depth & Colourspace",
            )
            if box_fig:
                st.plotly_chart(box_fig, width='stretch')

            # Box plot by resolution
            box_fig_res = build_time_box(
                filtered, "resolution_group",
                "Encoding Time Distribution by Resolution Group",
            )
            if box_fig_res:
                st.plotly_chart(box_fig_res, width='stretch')

            # Scatter: size vs time coloured by bitdepth/cs
            scatter_fig = build_time_vs_size_scatter(filtered, "bitdepth_cs")
            if scatter_fig:
                st.plotly_chart(scatter_fig, width='stretch')

            # Per-sequence encoding time bar
            st.subheader("Per-Sequence Encoding Duration")
            time_bar = go.Figure()
            time_bar.add_trace(go.Bar(
                x=valid_df["seq_id"],
                y=valid_df["encoding_minutes"],
                marker_color="#8b5cf6",
                text=valid_df["encoding_minutes"].round(1),
                textposition="outside",
            ))
            time_bar.update_layout(
                title="Encoding Duration per Sequence",
                xaxis_title="Sequence",
                yaxis_title="Duration (minutes)",
                showlegend=False,
            )
            st.plotly_chart(time_bar, width='stretch')

    # ── TAB 5: Data Table ─────────────────────────────
    with tab5:
        st.subheader("Raw Data")
        display_df = filtered[
            [
                "seq_id", "colourspace", "bitdepth",
                "image_width", "image_height",
                "resolution_group", "bitdepth_cs",
                "seq_size", "derivative_size",
                "savings", "savings_pct",
                "assessment_complete", "encoding_complete",
                "encoding_minutes",
            ]
        ].copy()
        display_df.columns = [
            "Sequence", "Colourspace", "Bit Depth",
            "Width", "Height",
            "Resolution Group", "Bit Depth / CS",
            "Before (bytes)", "After (bytes)",
            "Savings (bytes)", "Savings %",
            "Encoding Start", "Encoding End",
            "Encoding (min)",
        ]
        st.dataframe(display_df, width='stretch', height=600)


if __name__ == "__main__":
    main()
