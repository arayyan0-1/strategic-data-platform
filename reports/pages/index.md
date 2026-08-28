---
title: Signal Evaluation Panel
---

A point-in-time factor panel over the US common-stock universe. Each signal is
scored by its information coefficient, its long-short quintile spread, its
turnover and its overlap with the other signals. Every number is gross of cost
and computed from the dbt marts.

## Information coefficient

The cross-sectional Spearman correlation between the signal at D and the forward
return, averaged over sessions.

```sql ic
select signal, horizon, n_days, mean_ic, t_stat, ic_autocorr_lag1
from ic_summary
order by signal, horizon
```

<DataTable data={ic} rows=27 rowShading=true>
  <Column id=signal/>
  <Column id=horizon/>
  <Column id=n_days title="Sessions"/>
  <Column id=mean_ic title="Mean IC" fmt='0.0000' contentType=colorscale scaleColor=blue/>
  <Column id=t_stat title="t (naive)" fmt='0.00'/>
  <Column id=ic_autocorr_lag1 title="IC AC(1)" fmt='0.00'/>
</DataTable>

The t assumes independent daily ICs. Where AC(1) is large, the 5 and 21 session
forward windows overlap and the naive t overstates. Use a Newey-West standard
error before you quote it.

## Long-short spread (winsorized, gross)

The top signal quintile less the bottom, equal weighted, rebalanced each session.
The forward return is winsorized to its 1st and 99th percentile before averaging.

```sql ls
select signal, date, cum_ls from ls_returns order by date
```

<LineChart data={ls} x=date y=cum_ls series=signal title="Cumulative long-short (sum of daily spreads)"/>

## Signal shape

A monotone rise from quintile 1 to 5 is the signal at work.

```sql shape
select signal, quintile, mean_fwd_5 from quantiles order by signal, quintile
```

<BarChart data={shape} x=quintile y=mean_fwd_5 series=signal type=grouped title="Mean 5-session forward return by signal quintile"/>

## Turnover

The fraction of the top quintile replaced each session. A strong IC with a high
turnover can still be uninvestable once cost is paid.

```sql to
select signal, avg_turnover from turnover order by avg_turnover desc
```

<BarChart data={to} x=signal y=avg_turnover swapXY=true title="Average one-session turnover of the top quintile"/>

## Overlap

The average cross-sectional rank correlation between each pair of signals. A pair
near one is one bet wearing two names.

```sql corr
select signal_a, signal_b, rank_corr from correlation order by signal_a, signal_b
```

<DataTable data={corr} rows=50>
  <Column id=signal_a/>
  <Column id=signal_b/>
  <Column id=rank_corr title="Rank corr" fmt='0.00' contentType=colorscale scaleColor=red/>
</DataTable>
