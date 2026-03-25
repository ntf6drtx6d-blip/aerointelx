import pandas as pd
import streamlit as st
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from core.db import get_conn, init_db
from crawler.task_generator import generate_tasks

init_db()
conn = get_conn()

st.set_page_config(layout='wide', page_title='AeroIntel')
st.title('🚜 AeroIntel')

# Worker status
st.header('Worker Status')
worker_df = pd.read_sql('SELECT * FROM worker_status', conn)
if worker_df.empty:
    st.warning('No worker running')
else:
    st.dataframe(worker_df, use_container_width=True)

# Control
st.header('Control')
col1, col2, col3 = st.columns(3)
with col1:
    if st.button('Generate Tasks'):
        generate_tasks()
        st.success('Tasks generated')
with col2:
    if st.button('Reset stuck tasks'):
        conn.execute("UPDATE crawl_tasks SET status='pending' WHERE status='running'")
        conn.commit()
        st.success('Reset done')
with col3:
    if st.button('Retry failed'):
        conn.execute("UPDATE crawl_tasks SET status='pending' WHERE status='failed'")
        conn.commit()
        st.success('Retry triggered')

# Tasks summary
st.header('Tasks Summary')
tasks_df = pd.read_sql('SELECT * FROM crawl_tasks ORDER BY task_id DESC', conn)
st.dataframe(tasks_df, use_container_width=True)

# Assets
st.header('Assets')
assets_df = pd.read_sql('SELECT * FROM assets ORDER BY asset_id DESC', conn)
st.dataframe(assets_df, use_container_width=True)

# Sources
st.header('Sources')
sources_df = pd.read_sql('SELECT * FROM sources ORDER BY source_id DESC', conn)
st.dataframe(sources_df, use_container_width=True)

# Errors
st.header('Errors')
errors_df = pd.read_sql('SELECT * FROM crawler_errors ORDER BY error_id DESC', conn)
st.dataframe(errors_df, use_container_width=True)

conn.close()
