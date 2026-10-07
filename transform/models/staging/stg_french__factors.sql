{# Fama-French daily factors and 12 industry returns, as fractions. crsp_month is the file
   version. #}

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
