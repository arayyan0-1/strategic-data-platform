{#
  The Fama-French daily factors (market less the risk-free rate, size, value,
  profitability, investment, momentum) as fractions. The library lags the market by
  about two months. crsp_month names the version of the files.
#}

select
    date,
    mkt_rf,
    smb,
    hml,
    rmw,
    cma,
    mom,
    rf,
    crsp_month,
    vendor_pull_date
from {{ current_table('french_factors') }}
