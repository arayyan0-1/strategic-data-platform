{# A returned row is a check of the par_bond_return macro that fails. (1) A yield that does not
   move earns the coupon over 12 months and nothing else, at any yield, zero and negative included.
   (2) The return is continuous across a yield of zero. (3) A rise in the yield lowers the return
   and a fall raises it. (4) With a whole number of half-years left (5 + 1/12 years bought, 10
   half-years left) the macro equals the explicit sum of the coupons and the principal, each
   discounted at 1 + y/2 per half-year, less 1, plus the coupon accrued in the month. #}
with flat (c, years) as (

    values (0.04::double, 10), (0.0::double, 5), (0.07::double, 30), (-0.01::double, 10),
           (0.02::double, 2), (0.05::double, 20)

), flat_checks as (

    select 'flat yield' as check_name, c, years,
           {{ par_bond_return('c', 'c', 'years') }} - c / 12 as error
    from flat

), zero_checks as (

    select 'zero yield' as check_name, 0.02::double as c, 5 as years,
           {{ par_bond_return('0.02::double', '0.0::double', '5') }}
           - {{ par_bond_return('0.02::double', '1e-8::double', '5') }} as error

), direction_checks as (

    select 'direction' as check_name, 0.04::double as c, 10 as years,
           case when {{ par_bond_return('0.04::double', '0.05::double', '10') }} < 0.04 / 12
                 and {{ par_bond_return('0.04::double', '0.03::double', '10') }} > 0.04 / 12
                then 0.0 else 1.0 end as error

), sum_checks as (

    select 'explicit sum' as check_name, 0.03::double as c, 5 + 1.0 / 12 as years,
           {{ par_bond_return('0.03::double', '0.05::double', '5 + 1.0 / 12') }}
           - ((select sum(0.03 / 2 * pow(1 + 0.05 / 2, -k)) from generate_series(1, 10) t(k))
              + pow(1 + 0.05 / 2, -10) - 1 + 0.03 / 12) as error

)

select * from flat_checks where abs(error) > 1e-12
union all select * from zero_checks where abs(error) > 1e-6
union all select * from direction_checks where error <> 0
union all select * from sum_checks where abs(error) > 1e-12
