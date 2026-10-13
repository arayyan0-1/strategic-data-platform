{# The total return of one month on a par bond with semiannual coupons, from month-end yields.
   Bought at par at the end of month m-1 with a coupon equal to the yield then (prev_yield) and
   `years` to maturity, it is priced at the end of month m at the yield of that month with
   years - 1/12 left. All yields are decimals.

   Price (without accrued interest), with N = 2 * (years - 1/12) half-years, which may be
   fractional because the fraction sits in the exponent:
       P = c/y + (1 - c/y) / (1 + y/2)^N
   This is the sum of N coupons c/2 and the principal, each discounted at 1 + y/2 per half-year.
   It is 1 whenever y = c, so a yield that does not move earns the coupon and nothing else.
   Return = P - 1 + c/12: the price change and the coupon accrued in the month. A yield of 0
   makes c/y undefined; the sum then has no discounting: P = 1 + c * (years - 1/12). #}
{% macro par_bond_return(prev_yield, yield, years) %}
    (case
        when abs(({{ yield }})) < 1e-9
            then 1 + ({{ prev_yield }}) * (({{ years }}) - 1.0 / 12)
        else ({{ prev_yield }}) / ({{ yield }})
             + (1 - ({{ prev_yield }}) / ({{ yield }}))
               * pow(1 + ({{ yield }}) / 2, -2 * (({{ years }}) - 1.0 / 12))
    end - 1 + ({{ prev_yield }}) / 12)
{% endmacro %}
