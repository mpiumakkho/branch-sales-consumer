-- Demo: what HQ has stored, one row per branch and day, with its lines.
select h.branch_code, h.sale_date, h.revision, h.total_amount,
       to_char(h.confirmed_at at time zone 'Asia/Bangkok', 'YYYY-MM-DD HH24:MI:SS') as confirmed_at_bkk,
       to_char(h.received_at  at time zone 'Asia/Bangkok', 'YYYY-MM-DD HH24:MI:SS') as received_at_bkk,
       string_agg(l.category_code || ' ' || l.amount || ' x' || l.quantity, ', ' order by l.category_code) as lines
  from branch_daily_sales h
  join branch_daily_sales_line l on l.branch_daily_sales_id = h.id
 group by h.id
 order by h.branch_code, h.sale_date;
