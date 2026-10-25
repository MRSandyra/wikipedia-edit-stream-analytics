#!/usr/bin/env python3
"""
Dashboard - Wikipedia Edit Stream Analytics
Streamlit dashboard untuk menampilkan data real-time dari gold tables.
"""

import os
import random
import time
from datetime import datetime, timedelta
from urllib.parse import quote

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

import duckdb

# Configuration
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/data/wikipedia.duckdb")
REFRESH_INTERVAL = int(os.getenv("DASHBOARD_REFRESH_INTERVAL", "5"))


def connect_duckdb_with_retry(path, read_only=False, retries=5, base_delay=0.2):
    """Open a DuckDB connection, retrying with jittered backoff on lock
    conflicts. DuckDB's embedded file allows only one process to hold it at
    a time, and consumer/aggregator/dashboard all touch the same file
    periodically — brief collisions are expected, not exceptional (Bug #33).
    """
    for attempt in range(retries):
        try:
            return duckdb.connect(path, read_only=read_only)
        except Exception as e:
            if "lock" not in str(e).lower() or attempt == retries - 1:
                raise
            time.sleep(base_delay * (attempt + 1) + random.uniform(0, base_delay))

# Page config
st.set_page_config(
    page_title="Wikipedia Edit Stream Analytics",
    page_icon="📝",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
    }
    .metric-value {
        font-size: 2.5rem;
        font-weight: bold;
        margin: 0.5rem 0;
    }
    .metric-label {
        font-size: 1rem;
        opacity: 0.9;
    }
</style>
""", unsafe_allow_html=True)


def get_duckdb_connection():
    """Open a short-lived read-only DuckDB connection.

    DuckDB allows only one process to hold a read-write connection (or any
    connection at all alongside a read-write one); a connection cached for
    the dashboard's whole lifetime would starve the consumer/aggregator
    writers indefinitely. So a fresh read-only connection is opened once per
    Streamlit rerun and closed before the next refresh (Bug #32).
    """
    return connect_duckdb_with_retry(DUCKDB_PATH, read_only=True)


def check_gold_tables(conn):
    """Check if gold tables exist"""
    try:
        tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        
        required_tables = [
            "gold_edit_volume_1min",
            "gold_top_pages_15min",
            "gold_suspicious_feed",
            "gold_bot_human_ratio_1min"
        ]
        
        return all(t in table_names for t in required_tables)
    except Exception as e:
        st.error(f"Error checking tables: {e}")
        return False


def get_edit_rate(conn):
    """Get edit rate (edits per second) for the last minute"""
    try:
        result = conn.execute("""
            SELECT COUNT(*) as total
            FROM enriched_edits
            WHERE processed_at >= NOW() - INTERVAL '1 minute'
        """).fetchone()
        return result[0] / 60.0 if result else 0
    except:
        return 0


def get_total_edits(conn):
    """Get total edits since start"""
    try:
        result = conn.execute("SELECT COUNT(*) FROM enriched_edits").fetchone()
        return result[0] if result else 0
    except:
        return 0


def get_active_wikis(conn):
    """Get number of active wikis"""
    try:
        result = conn.execute("""
            SELECT COUNT(DISTINCT wiki)
            FROM enriched_edits
            WHERE processed_at >= NOW() - INTERVAL '5 minutes'
        """).fetchone()
        return result[0] if result else 0
    except:
        return 0


def get_suspicious_percentage(conn):
    """Get percentage of suspicious edits"""
    try:
        result = conn.execute("""
            SELECT 
                SUM(CASE WHEN is_suspicious THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as pct
            FROM enriched_edits
        """).fetchone()
        return result[0] if result and result[0] else 0
    except:
        return 0


def display_kpi_row(conn):
    """Display KPI metrics row"""
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        edit_rate = get_edit_rate(conn)
        st.metric(
            label="📝 Edit Rate (per sec)",
            value=f"{edit_rate:.1f}"
        )

    with col2:
        total_edits = get_total_edits(conn)
        st.metric(
            label="📊 Total Edits",
            value=f"{total_edits:,}"
        )

    with col3:
        active_wikis = get_active_wikis(conn)
        st.metric(
            label="🌐 Active Wikis",
            value=active_wikis
        )

    with col4:
        suspicious_pct = get_suspicious_percentage(conn)
        st.metric(
            label="⚠️ Suspicious %",
            value=f"{suspicious_pct:.1f}%"
        )


def display_edit_volume_chart(conn):
    """Display edit volume chart for last 30 minutes"""
    st.subheader("📈 Edit Volume (30 minutes)")

    try:
        # Check if table exists
        tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
        
        if "gold_edit_volume_1min" in tables:
            df = conn.execute("""
                SELECT minute, wiki, edit_count
                FROM gold_edit_volume_1min
                ORDER BY minute
            """).fetchdf()
            
            if not df.empty:
                fig = px.line(
                    df, 
                    x="minute", 
                    y="edit_count",
                    color="wiki",
                    labels={"edit_count": "Edits", "minute": "Time"},
                    title="Edit Volume by Wiki"
                )
                fig.update_layout(showlegend=True, height=400)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No data available yet. Waiting for edits...")
        else:
            st.info("Gold table not initialized yet. Waiting for data...")
    except Exception as e:
        st.warning(f"Error loading edit volume data: {e}")


def display_top_pages_chart(conn):
    """Display top 10 most edited pages"""
    st.subheader("🏆 Top 10 Most Edited Pages (15 minutes)")

    try:
        tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
        
        if "gold_top_pages_15min" in tables:
            df = conn.execute("""
                SELECT title, edit_count, unique_editors
                FROM gold_top_pages_15min
                LIMIT 10
            """).fetchdf()
            
            if not df.empty:
                fig = px.bar(
                    df,
                    x="title",
                    y="edit_count",
                    hover_data=["unique_editors"],
                    labels={"edit_count": "Edits", "title": "Page"},
                    title="Top 10 Most Edited Pages"
                )
                fig.update_layout(height=400, xaxis_title="")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No page edit data yet. Waiting for edits...")
        else:
            st.info("Gold table not initialized yet. Waiting for data...")
    except Exception as e:
        st.warning(f"Error loading top pages data: {e}")


def display_suspicious_feed(conn):
    """Display recent suspicious edits"""
    st.subheader("🚨 Recent Suspicious Edits")

    try:
        tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
        
        if "gold_suspicious_feed" in tables:
            df = conn.execute("""
                SELECT 
                    id,
                    processed_at,
                    wiki,
                    title,
                    user,
                    suspicion_score,
                    comment
                FROM gold_suspicious_feed
                LIMIT 50
            """).fetchdf()
            
            if not df.empty:
                # Create display dataframe with Wikipedia links
                display_df = df.copy()
                
                # Create wiki link
                def create_link(row):
                    wiki = row["wiki"]
                    title = quote(row["title"].replace(" ", "_"))
                    return f"https://{wiki}.wikipedia.org/wiki/{title}"
                
                display_df["Page Link"] = display_df.apply(create_link, axis=1)
                display_df["Time"] = display_df["processed_at"].dt.strftime("%Y-%m-%d %H:%M:%S")
                
                # Display with columns
                st.dataframe(
                    display_df[["Time", "wiki", "title", "user", "suspicion_score", "comment", "Page Link"]],
                    use_container_width=True,
                    column_config={
                        "Page Link": st.column_config.LinkColumn("Wikipedia Link"),
                        "suspicion_score": st.column_config.ProgressColumn(
                            "Score",
                            min_value=0,
                            max_value=100
                        )
                    }
                )
            else:
                st.info("No suspicious edits detected yet. Waiting for data...")
        else:
            st.info("Gold table not initialized yet. Waiting for data...")
    except Exception as e:
        st.warning(f"Error loading suspicious feed data: {e}")


def display_bot_human_ratio(conn):
    """Display bot vs human edit ratio"""
    st.subheader("🤖 Bot vs Human Edit Ratio")

    try:
        tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
        
        if "gold_bot_human_ratio_1min" in tables:
            df = conn.execute("""
                SELECT minute, wiki, bot_count, human_count
                FROM gold_bot_human_ratio_1min
                ORDER BY minute
            """).fetchdf()
            
            if not df.empty:
                fig = go.Figure()
                
                for wiki in df["wiki"].unique():
                    wiki_df = df[df["wiki"] == wiki]
                    fig.add_trace(go.Scatter(
                        x=wiki_df["minute"],
                        y=wiki_df["human_count"],
                        name=f"{wiki} (Human)",
                        mode="lines",
                        line=dict(color="blue")
                    ))
                    fig.add_trace(go.Scatter(
                        x=wiki_df["minute"],
                        y=wiki_df["bot_count"],
                        name=f"{wiki} (Bot)",
                        mode="lines",
                        line=dict(color="red", dash="dash")
                    ))
                
                fig.update_layout(
                    xaxis_title="Time",
                    yaxis_title="Edits",
                    height=400
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No bot/human ratio data yet. Waiting for edits...")
        else:
            st.info("Gold table not initialized yet. Waiting for data...")
    except Exception as e:
        st.warning(f"Error loading bot/human ratio data: {e}")


def main():
    """Main dashboard function"""
    # Header
    st.title("📊 Wikipedia Edit Stream Analytics")
    st.markdown("Real-time monitoring of Wikipedia edits with vandalism detection")
    st.markdown("---")

    try:
        conn = get_duckdb_connection()
    except Exception:
        # DB file doesn't exist yet (consumer hasn't started writing) or is
        # briefly locked by a consumer/aggregator flush — just retry next rerun.
        conn = None

    try:
        if conn is None or not check_gold_tables(conn):
            st.warning("⏳ Waiting for gold tables to be created by the aggregator...")
            st.info("This typically takes about 1-2 minutes after the first edits are processed.")
        else:
            # KPI Row
            display_kpi_row(conn)
            st.markdown("---")

            # Charts
            col1, col2 = st.columns(2)

            with col1:
                display_edit_volume_chart(conn)

            with col2:
                display_bot_human_ratio(conn)

            st.markdown("---")

            # Top pages
            display_top_pages_chart(conn)
            st.markdown("---")

            # Suspicious feed
            display_suspicious_feed(conn)
    finally:
        if conn is not None:
            conn.close()

    # Auto-refresh
    time.sleep(REFRESH_INTERVAL)
    st.rerun()


if __name__ == "__main__":
    main()