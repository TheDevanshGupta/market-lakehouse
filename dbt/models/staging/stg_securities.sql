select
    upper(trim(symbol)) as symbol,
    company_name,
    sector,
    industry
from {{ ref('securities') }}
