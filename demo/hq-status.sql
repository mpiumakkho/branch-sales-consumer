-- Demo: what HQ has stored, one row per branch and key, with its lines: the sales, the returns, then the shift
-- closes (one per terminal and shift, lines by tender; over/short = cash counted - cash expected).
select *
  from (
select 'SALES' as record, h.branch_code, h.sale_date as business_date, h.revision, h.total_amount,
       null as terminal_shift, null::numeric as cash_over_short,
       to_char(h.confirmed_at at time zone 'Asia/Bangkok', 'YYYY-MM-DD HH24:MI:SS') as confirmed_at_bkk,
       to_char(h.received_at  at time zone 'Asia/Bangkok', 'YYYY-MM-DD HH24:MI:SS') as received_at_bkk,
       string_agg(l.category_code || ' ' || l.amount || ' x' || l.quantity, ', ' order by l.category_code) as lines
  from branch_daily_sales h
  join branch_daily_sales_line l on l.branch_daily_sales_id = h.id
 group by h.id
union all
select 'RETURN', h.branch_code, h.return_date, h.revision, h.total_amount,
       null, null,
       to_char(h.confirmed_at at time zone 'Asia/Bangkok', 'YYYY-MM-DD HH24:MI:SS'),
       to_char(h.received_at  at time zone 'Asia/Bangkok', 'YYYY-MM-DD HH24:MI:SS'),
       string_agg(l.category_code || ' ' || l.amount || ' x' || l.quantity, ', ' order by l.category_code)
  from branch_daily_return h
  join branch_daily_return_line l on l.branch_daily_return_id = h.id
 group by h.id
union all
-- left join: a shift with no transactions has no tender lines
select 'SHIFT', h.branch_code, h.business_date, h.revision, h.total_amount,
       h.terminal_id || '#' || h.shift_no, h.cash_over_short,
       to_char(h.confirmed_at at time zone 'Asia/Bangkok', 'YYYY-MM-DD HH24:MI:SS'),
       to_char(h.received_at  at time zone 'Asia/Bangkok', 'YYYY-MM-DD HH24:MI:SS'),
       string_agg(t.tender_type || ' ' || t.amount || ' x' || t.quantity, ', ' order by t.tender_type)
  from branch_shift_close h
  left join branch_shift_close_tender t on t.branch_shift_close_id = h.id
 group by h.id
       ) stored
 order by branch_code, business_date, case record when 'SALES' then 1 when 'RETURN' then 2 else 3 end, terminal_shift;
