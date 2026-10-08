{# Monitor scores. A score of k is as rare, in its family's history, as a k-sigma move of a
   normal. move_z divides a move by trailing volatility, calibrated maps the
   ratio to its rarity in the pool, two_sided_z is the normal quantile. #}

{# A polynomial in x by Horner's rule. #}
{% macro horner(coefficients, x) -%}
    {%- set ns = namespace(expr='(' ~ coefficients[0] ~ ')') -%}
    {%- for c in coefficients[1:] -%}
        {%- set ns.expr = '(' ~ ns.expr ~ ' * ' ~ x ~ ' + (' ~ c ~ '))' -%}
    {%- endfor -%}
    {{ ns.expr }}
{%- endmacro %}

{# The z with P(|Z| >= z) = q for a standard normal (0.0027 gives 3). q names a column with
   values in (0, 1]. Rational approximation, relative error under 1.2e-9. #}
{% macro two_sided_z(q) -%}
    {%- set a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
                 1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00] -%}
    {%- set b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
                 6.680131188771972e+01, -1.328068155288572e+01, 1] -%}
    {%- set c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
                 -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00] -%}
    {%- set d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
                 3.754408661907416e+00, 1] -%}
    {%- set tail = 'sqrt(-2 * ln(' ~ q ~ ' / 2))' -%}
    {%- set centre = '(' ~ q ~ ' / 2 - 0.5)' -%}
    case
        when {{ q }} < 0.0485
            then -({{ horner(c, tail) }} / {{ horner(d, tail) }})
        else -({{ horner(a, '(' ~ centre ~ ' * ' ~ centre ~ ')') }} * {{ centre }}
               / {{ horner(b, '(' ~ centre ~ ' * ' ~ centre ~ ')') }})
    end
{%- endmacro %}

{# Trailing z of the move over each h in horizons. moves has family, subject, date, x
   (one-session change), min_sd (volatility floor) and elig (in the pool). Volatility is an
   exponentially weighted RMS of earlier x (monitor_halflife, _vol_window, _min_obs). The running
   weights overflow after about 1,000 half-lives. #}
{% macro move_z(moves, horizons) -%}
    {%- set lam = 0.5 ** (1.0 / var('monitor_halflife')) -%}
    (
    with indexed as (
        select family, subject, date, x, min_sd, elig, x * x as sq,
               row_number() over (partition by family, subject order by date) as i
        from {{ moves }}
    ), scaled as (
        select
            *,
            case when count(sq) over ew >= {{ var('monitor_min_obs') }}
                 then sqrt(sum(sq * pow({{ lam }}, -i)) over ew * pow({{ lam }}, i - 1)
                           * (1 - {{ lam }}) / (1 - pow({{ lam }}, count(sq) over ew)))
            end as sd_raw
        from indexed
        window ew as (partition by family, subject order by date
                      rows between {{ var('monitor_vol_window') }} preceding and 1 preceding)
    ), vol as (
        select *, case when sd_raw is not null then greatest(sd_raw, min_sd) end as sd
        from scaled
    )
    {% for h in horizons %}
    select
        family, subject, {{ h }} as h, date, elig,
        case when count(x) over mv{{ h }} = {{ h }}
             then sum(x) over mv{{ h }} / nullif(sd * {{ h ** 0.5 }}, 0) end as z_raw
    from vol
    window mv{{ h }} as (partition by family, subject order by date
                        rows between {{ h - 1 }} preceding and current row)
    {{ "union all" if not loop.last }}
    {% endfor %}
    )
{%- endmacro %}

{# Score of each row of raw (family, subject, h, date, elig, z_raw); only elig rows form a pool
   per family and horizon. q is the share of the pool at least as large as |z_raw|, counting half
   the row itself (the largest of n has q = 0.5 / n). #}
{% macro calibrated(raw) -%}
    (
    with pool as (
        select family, subject, h, date, z_raw, abs(z_raw) as a
        from {{ raw }}
        where elig and isfinite(z_raw)
    ), ranked as (
        select
            *,
            (greatest(count(*) over (partition by family, h order by a desc
                                     range between unbounded preceding and current row), 1) - 0.5)
                / count(*) over (partition by family, h) as q
        from pool
    )
    select family, subject, h, date, z_raw, sign(z_raw) * {{ two_sided_z('q') }} as z
    from ranked
    )
{%- endmacro %}
