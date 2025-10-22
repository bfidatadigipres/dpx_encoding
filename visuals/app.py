import os
import sqlite3
import sys

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


def get_data(database):
    """
    Fetch file size data from encoding_database
    """
    conn = sqlite3.connect(database)
    query = "SELECT seq_id, seq_size, derivative_size FROM encoding_status WHERE status = 'MKV validation complete'"
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def calculate_savings(df):
    """
    Make size different calc
    """
    df["savings"] = df["seq_size"] - df["derivative_size"]
    return df


def main():
    """
    Outputs the steamlit app
    """
    st.title("RAWcooked compression storage savings chart")
    database = os.environ.get("DATABASE", None)
    if database is None:
        sys.exit("Database could not be found.")

    if st.button("Load Data"):
        try:
            df = get_data(database)
            df = calculate_savings(df)

            # Create plotly barchart
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=df["seq_id"],
                    y=df["seq_size"],
                    name="Size before (bytes)",
                    marker_color="blue",
                    text=df["seq_size"],
                    textposition="outside",
                )
            )
            fig.add_trace(
                go.Bar(
                    x=df["seq_id"],
                    y=df["derivative_size"],
                    name="Size after (bytes)",
                    marker_color="yellow",
                    text=df["derivative_size"],
                    textposition="outside",
                )
            )
            fig.add_trace(
                go.Bar(
                    x=df["seq_id"],
                    y=df["savings"],
                    name="Savings (bytes)",
                    marker_color="red",
                    text=df["savings"],
                    textposition="outside",
                )
            )
            fig.update_layout(
                title="RAWcooked File sizes and savings",
                xaxis_title="File Name",
                yaxis_title="Sizes",
                barmode="group",
                hovermode="x unified",
                showlegend=True,
            )
            # Display the Plotly figure
            st.plotly_chart(fig, use_container_width=True)

            # Calculcate total size in terabtyes
            total_before = df["seq_size"].sum() / (1024**4)
            total_after = df["derivative_size"].sum() / (1024**4)
            total_saved = df["savings"].sum() / (1024**4)
            total_files = df["seq_id"].nunique()

            mini_fig = go.Figure(
                data=[
                    go.Pie(
                        labels=[
                            "Total size before (TB)",
                            "Total size after (TB)",
                            "Savings (TB)",
                        ],
                        values=[total_before, total_after, total_saved],
                        hole=0.4,
                        marker=dict(colors=["blue", "yellow", "red"]),
                        textinfo="label+percent",
                    )
                ]
            )
            mini_fig.update_layout(title_text="Total Size (TB)", showlegend=True)
            # Display as mini pie chart
            st.plotly_chart(mini_fig, use_container_width=True)
            st.markdown(f"Total sequences RAWcooked: {total_files}")

        except Exception as err:
            st.error(f"Error: {err}")


if __name__ == "__main__":
    main()
