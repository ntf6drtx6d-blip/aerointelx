import datetime
import pandas as pd
import streamlit as st

from core.db import get_conn
from core.utils import now_utc, safe_json_dumps

st.set_page_config(layout="wide", page_title="AeroIntel Registry")
st.title("AeroIntel Registry Dashboard")


# =========================
# DB helpers
# =========================
@st.cache_data(ttl=30)
def read_df(query: str) -> pd.DataFrame:
    conn = get_conn()
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()


def execute_sql(query: str, params=None):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(query, params or ())
        conn.commit()
    finally:
        conn.close()


def execute_fetchone(query: str, params=None):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(query, params or ())
        row = cur.fetchone()
        conn.commit()
        return row
    finally:
        conn.close()


def clear_cache():
    read_df.clear()


# =========================
# Sidebar controls
# =========================
st.sidebar.header("Crawler Control")

with st.sidebar.expander("Create New Job", expanded=True):
    job_name = st.text_input("Job name", value="manual-job")

    all_countries_df = read_df("SELECT country_code, country_name FROM countries ORDER BY country_code")
    country_options = all_countries_df["country_code"].tolist() if not all_countries_df.empty else ["BR", "MX", "CO"]
    selected_countries = st.multiselect("Countries", country_options, default=country_options[:1])

    asset_type_options = ["airport", "airstrip", "military_base"]
    selected_asset_types = st.multiselect("Asset types", asset_type_options, default=["airport"])

    entity_type_options = ["operator", "municipality", "ministry", "mining_company", "military_authority"]
    selected_entity_types = st.multiselect("Entity types", entity_type_options, default=["operator", "municipality", "ministry"])

    mode = st.selectbox("Mode", ["broad", "focused"], index=0)
    run_interval_minutes = st.number_input("Run interval (minutes)", min_value=1, max_value=1440, value=60, step=5)
    max_tasks_per_run = st.number_input("Max tasks per run", min_value=1, max_value=500, value=10, step=1)
    requests_per_minute = st.number_input("Requests per minute", min_value=1, max_value=500, value=20, step=1)
    enabled = st.checkbox("Enabled", value=True)

    if st.button("Create Job", use_container_width=True):
        if not selected_countries:
            st.sidebar.error("Select at least one country")
        else:
            execute_sql("""
                INSERT INTO crawl_jobs (
                    job_name, countries_json, asset_types_json, entity_types_json, mode,
                    enabled, run_interval_minutes, max_tasks_per_run, requests_per_minute,
                    created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                job_name,
                safe_json_dumps(selected_countries),
                safe_json_dumps(selected_asset_types),
                safe_json_dumps(selected_entity_types),
                mode,
                1 if enabled else 0,
                run_interval_minutes,
                max_tasks_per_run,
                requests_per_minute,
                now_utc(),
                now_utc(),
            ))
            clear_cache()
            st.sidebar.success("Job created")

with st.sidebar.expander("Task Actions", expanded=True):
    if st.button("Retry Failed Tasks", use_container_width=True):
        execute_sql("""
            UPDATE crawl_tasks
            SET status = 'pending',
                updated_at = %s
            WHERE status = 'failed'
        """, (now_utc(),))
        clear_cache()
        st.sidebar.success("Failed tasks moved to pending")

    if st.button("Reset Running Tasks", use_container_width=True):
        execute_sql("""
            UPDATE crawl_tasks
            SET status = 'pending',
                started_at = NULL,
                updated_at = %s,
                notes = 'reset from app'
            WHERE status = 'running'
        """, (now_utc(),))
        clear_cache()
        st.sidebar.success("Running tasks reset")

    if st.button("Delete Pending Tasks", use_container_width=True):
        execute_sql("""
            DELETE FROM crawl_tasks
            WHERE status = 'pending'
        """)
        clear_cache()
        st.sidebar.success("Pending tasks deleted")

with st.sidebar.expander("Job Actions", expanded=True):
    jobs_df_sidebar = read_df("""
        SELECT job_id, job_name, enabled, mode, run_interval_minutes, max_tasks_per_run, requests_per_minute
        FROM crawl_jobs
        ORDER BY job_id DESC
    """)

    if jobs_df_sidebar.empty:
        st.sidebar.info("No jobs yet")
    else:
        selected_job_id = st.selectbox("Select job", jobs_df_sidebar["job_id"].tolist())

        col_a, col_b = st.columns(2)

        with col_a:
            if st.button("Enable Job", use_container_width=True):
                execute_sql("""
                    UPDATE crawl_jobs
                    SET enabled = 1, updated_at = %s
                    WHERE job_id = %s
                """, (now_utc(), selected_job_id))
                clear_cache()
                st.sidebar.success(f"Job {selected_job_id} enabled")

        with col_b:
            if st.button("Disable Job", use_container_width=True):
                execute_sql("""
                    UPDATE crawl_jobs
                    SET enabled = 0, updated_at = %s
                    WHERE job_id = %s
                """, (now_utc(), selected_job_id))
                clear_cache()
                st.sidebar.success(f"Job {selected_job_id} disabled")

        if st.button("Create Tasks For Selected Job", use_container_width=True):
            row = execute_fetchone("""
                SELECT countries_json, asset_types_json, entity_types_json, max_tasks_per_run
                FROM crawl_jobs
                WHERE job_id = %s
            """, (selected_job_id,))

            if row:
                import json
                countries = json.loads(row[0]) if row[0] else []
                asset_types = json.loads(row[1]) if row[1] else []
                entity_types = json.loads(row[2]) if row[2] else []
                max_tasks = row[3] or 10

                created = 0
                for country_code in countries:
                    if asset_types:
                        for asset_type in asset_types:
                            if entity_types:
                                for entity_type in entity_types:
                                    if created >= max_tasks:
                                        break
                                    execute_sql("""
                                        INSERT INTO crawl_tasks (
                                            job_id, country_code, asset_type, entity_type,
                                            status, retries, created_at, updated_at
                                        )
                                        VALUES (%s, %s, %s, %s, 'pending', 0, %s, %s)
                                    """, (
                                        selected_job_id,
                                        country_code,
                                        asset_type,
                                        entity_type,
                                        now_utc(),
                                        now_utc(),
                                    ))
                                    created += 1
                            else:
                                if created >= max_tasks:
                                    break
                                execute_sql("""
                                    INSERT INTO crawl_tasks (
                                        job_id, country_code, asset_type,
                                        status, retries, created_at, updated_at
                                    )
                                    VALUES (%s, %s, %s, 'pending', 0, %s, %s)
                                """, (
                                    selected_job_id,
                                    country_code,
                                    asset_type,
                                    now_utc(),
                                    now_utc(),
                                ))
                                created += 1
                    else:
                        if created >= max_tasks:
                            break
                        execute_sql("""
                            INSERT INTO crawl_tasks (
                                job_id, country_code,
                                status, retries, created_at, updated_at
                            )
                            VALUES (%s, %s, 'pending', 0, %s, %s)
                        """, (
                            selected_job_id,
                            country_code,
                            now_utc(),
                            now_utc(),
                        ))
                        created += 1

                clear_cache()
                st.sidebar.success(f"Created {created} tasks")

# =========================
# Top KPIs
# =========================
assets_count = int(read_df("SELECT COUNT(*) AS c FROM assets").iloc[0]["c"])
entities_count = int(read_df("SELECT COUNT(*) AS c FROM entities").iloc[0]["c"])
links_count = int(read_df("SELECT COUNT(*) AS c FROM asset_entity_links").iloc[0]["c"])
sources_count = int(read_df("SELECT COUNT(*) AS c FROM sources").iloc[0]["c"])
tasks_count = int(read_df("SELECT COUNT(*) AS c FROM crawl_tasks").iloc[0]["c"])
jobs_count = int(read_df("SELECT COUNT(*) AS c FROM crawl_jobs").iloc[0]["c"])

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Assets", assets_count)
k2.metric("Entities", entities_count)
k3.metric("Links", links_count)
k4.metric("Sources", sources_count)
k5.metric("Tasks", tasks_count)
k6.metric("Jobs", jobs_count)

st.divider()

# =========================
# Worker health
# =========================
st.subheader("Worker Health")

worker_df = read_df("""
    SELECT worker_id, last_heartbeat, current_task, processed_tasks, errors, updated_at
    FROM worker_status
    ORDER BY updated_at DESC
""")

if worker_df.empty:
    st.warning("No worker heartbeat yet")
else:
    last_row = worker_df.iloc[0]
    try:
        last_ts = pd.to_datetime(last_row["updated_at"], utc=True)
        now_ts = pd.Timestamp.utcnow()
        delta_sec = (now_ts - last_ts).total_seconds()

        if delta_sec < 120:
            st.success("Worker is alive")
        elif delta_sec < 600:
            st.warning("Worker is slow")
        else:
            st.error("Worker might be stuck")
    except Exception:
        st.info("Worker heartbeat present, but could not calculate health")

    st.dataframe(worker_df, use_container_width=True)

st.divider()

# =========================
# Task summary
# =========================
st.subheader("Task Summary")
task_summary = read_df("""
    SELECT status, COUNT(*) AS count
    FROM crawl_tasks
    GROUP BY status
    ORDER BY status
""")
st.dataframe(task_summary, use_container_width=True)

st.divider()

# =========================
# Country coverage
# =========================
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

st.divider()

# =========================
# Jobs
# =========================
st.subheader("Jobs")
jobs_df = read_df("""
    SELECT job_id, job_name, countries_json, asset_types_json, entity_types_json,
           mode, enabled, run_interval_minutes, max_tasks_per_run,
           requests_per_minute, last_run_at, created_at, updated_at
    FROM crawl_jobs
    ORDER BY job_id DESC
""")
st.dataframe(jobs_df, use_container_width=True)

st.divider()

# =========================
# Filters
# =========================
st.subheader("Registry Explorer")

country_options_df = read_df("SELECT DISTINCT country_code FROM countries ORDER BY country_code")
country_options = ["ALL"] + country_options_df["country_code"].tolist() if not country_options_df.empty else ["ALL"]
selected_country = st.selectbox("Filter by country", country_options)

if selected_country == "ALL":
    assets_query = """
        SELECT asset_id, country_code, asset_name, asset_type, city, region, status, canonical_source_url, discovered_at, updated_at
        FROM assets
        ORDER BY discovered_at DESC
        LIMIT 300
    """
    entities_query = """
        SELECT entity_id, country_code, entity_name, entity_type, official_domain, notes, discovered_at, updated_at
        FROM entities
        ORDER BY discovered_at DESC
        LIMIT 300
    """
    errors_query = """
        SELECT created_at, country_code, seed_name, url, stage, error_text
        FROM crawler_errors
        ORDER BY created_at DESC
        LIMIT 200
    """
else:
    assets_query = f"""
        SELECT asset_id, country_code, asset_name, asset_type, city, region, status, canonical_source_url, discovered_at, updated_at
        FROM assets
        WHERE country_code = '{selected_country}'
        ORDER BY discovered_at DESC
        LIMIT 300
    """
    entities_query = f"""
        SELECT entity_id, country_code, entity_name, entity_type, official_domain, notes, discovered_at, updated_at
        FROM entities
        WHERE country_code = '{selected_country}'
        ORDER BY discovered_at DESC
        LIMIT 300
    """
    errors_query = f"""
        SELECT created_at, country_code, seed_name, url, stage, error_text
        FROM crawler_errors
        WHERE country_code = '{selected_country}'
        ORDER BY created_at DESC
        LIMIT 200
    """

tab1, tab2, tab3, tab4 = st.tabs(["Assets", "Entities", "Links", "Errors"])

with tab1:
    assets_df = read_df(assets_query)
    st.dataframe(assets_df, use_container_width=True)

with tab2:
    entities_df = read_df(entities_query)
    st.dataframe(entities_df, use_container_width=True)

with tab3:
    links_df = read_df("""
        SELECT link_id, asset_id, entity_id, role, source_url, source_quote, discovered_at, updated_at
        FROM asset_entity_links
        ORDER BY updated_at DESC NULLS LAST, discovered_at DESC
        LIMIT 300
    """)
    st.dataframe(links_df, use_container_width=True)

with tab4:
    errors_df = read_df(errors_query)
    st.dataframe(errors_df, use_container_width=True)

st.divider()

# =========================
# Recent tasks
# =========================
st.subheader("Recent Tasks")
recent_tasks_df = read_df("""
    SELECT task_id, job_id, country_code, asset_type, entity_type, status, retries,
           created_at, started_at, finished_at, updated_at, notes
    FROM crawl_tasks
    ORDER BY task_id DESC
    LIMIT 200
""")
st.dataframe(recent_tasks_df, use_container_width=True)
