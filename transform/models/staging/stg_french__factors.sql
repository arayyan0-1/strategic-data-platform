{#
  The Fama-French daily factors (market less the risk-free rate, size, value,
  profitability, investment, momentum, short- and long-term reversal) and the
  value-weighted returns of the 12 industry portfolios, as fractions. The library lags
  the market by about two months. crsp_month names the version of the files.
#}

select
    date,
    mkt_rf,
    smb,
    hml,
    rmw,
    cma,
    mom,
    st_rev,
    lt_rev,
    rf,
    ind_nodur, ind_durbl, ind_manuf, ind_enrgy, ind_chems, ind_buseq,
    ind_telcm, ind_utils, ind_shops, ind_hlth, ind_money, ind_other,
    crsp_month,
    vendor_pull_date
from {{ source('french', 'factors') }}
