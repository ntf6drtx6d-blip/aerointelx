import pandas as pd
import streamlit as st
from core.db import get_conn, init_db
from core.utils import now_utc, safe_json_dumps
from configs.countries import COUNTRIES

st.set_page_config(layout="wide", page_title="AeroIntel Registry")
init_db()
st.title("🚜 AeroIntel Registry Control Panel")
st.caption("Phase 1 — registry only. No signals yet.")
conn = get_conn()

tabs = st.tabs(["Overview", "Countries", "Assets", "Entities", "Links", "Jobs", "Tasks", "Worker", "Errors"])

with tabs[0]:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Assets", int(pd.read_sql("SELECT COUNT(*) AS n FROM assets", conn).iloc[0]["n"]))
    c2.metric("Entities", int(pd.read_sql("SELECT COUNT(*) AS n FROM entities", conn).iloc[0]["n"]))
    c3.metric("Links", int(pd.read_sql("SELECT COUNT(*) AS n FROM asset_entity_links", conn).iloc[0]["n"]))
    c4.metric("Sources", int(pd.read_sql("SELECT COUNT(*) AS n FROM sources", conn).iloc[0]["n"]))
    st.subheader("Coverage by country")
    coverage_sql = """
    SELECT c.country_code, c.country_name,
           COALESCE(a.asset_count, 0) AS assets,
           COALESCE(e.entity_count, 0) AS entities,
           COALESCE(s.source_count, 0) AS sources
    FROM countries c
    LEFT JOIN (SELECT country_code, COUNT(*) AS asset_count FROM assets GROUP BY country_code) a ON a.country_code = c.country_code
    LEFT JOIN (SELECT country_code, COUNT(*) AS entity_count FROM entities GROUP BY country_code) e ON e.country_code = c.country_code
    LEFT JOIN (SELECT country_code, COUNT(*) AS source_count FROM sources GROUP BY country_code) s ON s.country_code = c.country_code
    ORDER BY c.country_name
    """
    st.dataframe(pd.read_sql(coverage_sql, conn), use_container_width=True)

with tabs[1]:
    st.dataframe(pd.read_sql("SELECT * FROM countries ORDER BY country_name", conn), use_container_width=True)

with tabs[2]:
    country_filter = st.selectbox("Country filter", ["All"] + [name for _, name in COUNTRIES], key="assets_country")
    if country_filter == "All":
        assets_df = pd.read_sql("SELECT * FROM assets ORDER BY country_code, asset_name", conn)
    else:
        code = next(code for code, name in COUNTRIES if name == country_filter)
        assets_df = pd.read_sql("SELECT * FROM assets WHERE country_code=? ORDER BY asset_name", conn, params=(code,))
    st.dataframe(assets_df, use_container_width=True)

with tabs[3]:
    st.dataframe(pd.read_sql("SELECT * FROM entities ORDER BY country_code, entity_name", conn), use_container_width=True)

with tabs[4]:
    links_sql = """
    SELECT l.link_id, a.country_code, a.asset_name, e.entity_name, e.entity_type, l.role, l.source_url, l.discovered_at
    FROM asset_entity_links l
    JOIN assets a ON a.asset_id = l.asset_id
    JOIN entities e ON e.entity_id = l.entity_id
    ORDER BY a.country_code, a.asset_name, e.entity_name
    """
    st.dataframe(pd.read_sql(links_sql, conn), use_container_width=True)

with tabs[5]:
    with st.expander("Create job", expanded=False):
        job_name = st.text_input("Job name", value="focused-job")
        selected_countries = st.multiselect("Countries", options=[code for code, _ in COUNTRIES], default=["BR"])
        selected_asset_types = st.multiselect("Asset types", options=["airport", "airstrip", "military_base"], default=["airport"])
        selected_entity_types = st.multiselect("Entity types", options=["operator", "municipality", "ministry", "mining_company", "military_authority"], default=["operator", "municipality", "ministry"])
        mode = st.selectbox("Mode", ["broad", "focused"])
        run_interval = st.number_input("Run interval minutes", min_value=5, max_value=1440, value=60)
        max_tasks = st.number_input("Max tasks per run", min_value=1, max_value=500, value=10)
        rpm = st.number_input("Requests per minute", min_value=1, max_value=300, value=20)
        if st.button("Create job"):
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO crawl_jobs (job_name, countries_json, asset_types_json, entity_types_json, mode, enabled, run_interval_minutes, max_tasks_per_run, requests_per_minute, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
                """,
                (job_name, safe_json_dumps(selected_countries), safe_json_dumps(selected_asset_types), safe_json_dumps(selected_entity_types), mode, int(run_interval), int(max_tasks), int(rpm), now_utc(), now_utc())
            )
            conn.commit()
            st.success("Job created")
    st.dataframe(pd.read_sql("SELECT * FROM crawl_jobs ORDER BY job_id DESC", conn), use_container_width=True)

with tabs[6]:
    col1, col2 = st.columns(2)
    if col1.button("Reset stuck running tasks"):
        conn.execute("UPDATE crawl_tasks SET status='pending', updated_at=? WHERE status='running'", (now_utc(),))
        conn.commit()
        st.success("Running tasks reset to pending")
    if col2.button("Retry failed tasks"):
        conn.execute("UPDATE crawl_tasks SET status='pending', updated_at=? WHERE status='failed'", (now_utc(),))
        conn.commit()
        st.success("Failed tasks reset to pending")
    st.dataframe(pd.read_sql("SELECT * FROM crawl_tasks ORDER BY task_id DESC", conn), use_container_width=True)

with tabs[7]:
    st.dataframe(pd.read_sql("SELECT * FROM worker_status", conn), use_container_width=True)

with tabs[8]:
    st.dataframe(pd.read_sql("SELECT * FROM crawler_errors ORDER BY error_id DESC", conn), use_container_width=True)

conn.close()
