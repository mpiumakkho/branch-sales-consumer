-- Demo: the shift closes of each branch and business day against the daily sales HQ has for that day. A query, not
-- a rule: HQ stores both whatever the difference (requirements R13). A difference means a shift not closed or not
-- sent yet, a day not confirmed yet, or figures the POS and the back-office count differently (e.g. returns).
with shifts as (
    select branch_code, business_date, count(*) as shifts, sum(total_amount) as shift_total,
           sum(cash_over_short) as cash_over_short
      from branch_shift_close
     group by branch_code, business_date
)
select coalesce(s.branch_code, d.branch_code)                 as branch_code,
       coalesce(s.business_date, d.sale_date)                 as business_date,
       coalesce(s.shifts, 0)                                  as shifts,
       s.shift_total,
       d.total_amount                                         as daily_sales_total,
       coalesce(s.shift_total, 0) - coalesce(d.total_amount, 0) as difference,
       s.cash_over_short
  from shifts s
  full join branch_daily_sales d on d.branch_code = s.branch_code and d.sale_date = s.business_date
 order by 1, 2;
