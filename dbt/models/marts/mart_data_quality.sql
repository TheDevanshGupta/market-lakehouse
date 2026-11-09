-- Observability: one row per pipeline run x DQ rule, with pass rates.
select
    r.run_id,
    r.started_at,
    r.data_source,
    r.start_date,
    r.end_date,
    r.status,
    r.rows_ingested,
    r.rows_valid,
    r.rows_quarantined,
    r.rows_duplicate,
    q.rule_name,
    q.rule_description,
    q.severity,
    q.rows_checked,
    q.rows_failed,
    1 - q.rows_failed / nullif(q.rows_checked, 0) as pass_rate
from {{ ref('stg_pipeline_runs') }} as r
left join {{ ref('stg_dq_rule_results') }} as q using (run_id)
