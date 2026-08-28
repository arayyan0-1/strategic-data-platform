---
title: Current Signals
---

```sql asof
select max(date) as as_of from snapshot
```

The present cross-sectional tilt of each signal, as of session <Value data={asof} column=as_of/>.
This is a description of where names sit now. It is not a recommendation to trade.

```sql signals
select distinct signal from snapshot order by signal
```

<Dropdown data={signals} name=sig value=signal defaultValue="reversal_5"/>

## Top decile of the signal

```sql top
select ticker, value, z, rank_high
from snapshot
where signal = '${inputs.sig.value}' and decile = 10
order by rank_high
limit 25
```

<DataTable data={top}>
  <Column id=ticker/>
  <Column id=value fmt='0.0000'/>
  <Column id=z title="z-score" fmt='0.00'/>
  <Column id=rank_high title="Rank"/>
</DataTable>

## Bottom decile of the signal

```sql bottom
select ticker, value, z
from snapshot
where signal = '${inputs.sig.value}' and decile = 1
order by value
limit 25
```

<DataTable data={bottom}>
  <Column id=ticker/>
  <Column id=value fmt='0.0000'/>
  <Column id=z title="z-score" fmt='0.00'/>
</DataTable>
