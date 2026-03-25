import pandas as pd
import streamlit as st
from core.db import init_db, get_conn

st.set_page_config(layout="wide", page_title="AeroIntel Registry")
init_db()
st.title("AeroIntel Registry Dashboard")
conn = get_conn()

def read_df(query):
    return pd.read_sql(query, conn)

st.subheader("System Overview")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Countries", int(read_df("SELECT COUNT(*) AS c FROM countries").iloc[0]["c"]))
col2.metric("Assets", int(read_df("SELECT COUNT(*) AS c FROM assets").iloc[0]["c"]))
col3.metric("Entities", int(read_df("SELECT COUNT(*) AS c FROM entities").iloc[0]["c"]))
col4.metric("Sources", int(read_df("SELECT COUNT(*) AS c FROM sources").iloc[0]["c"]))

st.subheader("Worker Status")
worker_df = read_df("SELECT * FROM worker_status ORDER BY updated_at DESC")
if worker_df.empty:
    st.info("No worker heartbeat yet")
else:
    st.dataframe(worker_df, use_container_width=True)

st.subheader("Task Summary")
task_summary = read_df("SELECT status, COUNT(*) AS count FROM crawl_tasks GROUP BY status ORDER BY status")
st.dataframe(task_summary, use_container_width=True)

st.subheader("Country Coverage")
country_df = read_df("""
SELECT
    c.country_code,
    c.country_name,
    COALESCE(a.asset_count, 0) AS assets,
    COALESCE(e.entity_count, 0) AS entities,
    COALESCE(s.source_count, 0) AS sources
FROM countries c
LEFT JOIN (
    SELECT country_code, COUNT(*) AS asset_count
    FROM assets
    GROUP BY country_code
) a ON a.country_code = c.country_code
LEFT JOIN (
    SELECT country_code, COUNT(*) AS entity_count
    FROM entities
    GROUP BY country_code
) e ON e.country_code = c.country_code
LEFT JOIN (
    SELECT country_code, COUNT(*) AS source_count
    FROM sources
    GROUP BY country_code
) s ON s.country_code = c.country_code
ORDER BY c.country_code
""")
st.dataframe(country_df, use_container_width=True)

st.subheader("Assets")
assets_df = read_df("""
SELECT asset_id, country_code, asset_name, asset_type, city, canonical_source_url, discovered_at
FROM assets
ORDER BY discovered_at DESC
LIMIT 200
""")
st.dataframe(assets_df, use_container_width=True)

st.subheader("Entities")
entities_df = read_df("""
SELECT entity_id, country_code, entity_name, entity_type, official_domain, discovered_at
FROM entities
ORDER BY discovered_at DESC
LIMIT 200
""")
st.dataframe(entities_df, use_container_width=True)

st.subheader("Recent Errors")
errors_df = read_df("""
SELECT created_at, country_code, seed_name, url, stage, error_text
FROM crawler_errors
ORDER BY created_at DESC
LIMIT 100
""")
st.dataframe(errors_df, use_container_width=True)

conn.close()
