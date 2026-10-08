-- Demo: what HQ has stored, one row per branch and day, with its lines: the sales, then the returns.
select 'SALES' as record, h.branch_code, h.sale_date as business_date, h.revision, h.total_amount,
       to_char(h.confirmed_at at time zone 'Asia/Bangkok', 'YYYY-MM-DD HH24:MI:SS') as confirmed_at_bkk,
       to_char(h.received_at  at time zone 'Asia/Bangkok', 'YYYY-MM-DD HH24:MI:SS') as received_at_bkk,
       string_agg(l.category_code || ' ' || l.amount || ' x' || l.quantity, ', ' order by l.category_code) as lines
  from branch_daily_sales h
  join branch_daily_sales_line l on l.branch_daily_sales_id = h.id
 group by h.id
union all
select 'RETURN', h.branch_code, h.return_date, h.revision, h.total_amount,
       to_char(h.confirmed_at at time zone 'Asia/Bangkok', 'YYYY-MM-DD HH24:MI:SS'),
       to_char(h.received_at  at time zone 'Asia/Bangkok', 'YYYY-MM-DD HH24:MI:SS'),
       string_agg(l.category_code || ' ' || l.amount || ' x' || l.quantity, ', ' order by l.category_code)
  from branch_daily_return h
  join branch_daily_return_line l on l.branch_daily_return_id = h.id
 group by h.id
 order by 2, 3, 1 desc;
