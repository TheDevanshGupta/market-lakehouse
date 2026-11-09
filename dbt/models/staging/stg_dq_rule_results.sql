select
    run_id,
    rule_name,
    description as rule_description,
    severity,
    rows_checked,
    rows_failed
from {{ source('audit', 'dq_rule_results') }}
