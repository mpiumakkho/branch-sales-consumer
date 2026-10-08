-- Tender master, owned by HQ like category. Source: contract/tender-types.md
create table tender_type (
    tender_type varchar(30)  primary key,
    description varchar(200) not null,
    created_at  timestamptz  not null default now()
);

insert into tender_type (tender_type, description) values
    ('CASH',        'Cash'),
    ('CREDIT_CARD', 'Credit card'),
    ('DEBIT_CARD',  'Debit card'),
    ('QR_PAYMENT',  'QR payment'),
    ('E_WALLET',    'E-wallet'),
    ('COUPON',      'Coupon or voucher'),
    ('OTHER',       'Other tender');

-- Shift close (Z-report) of one POS terminal (contract ShiftClose): one row per
-- (branch_code, business_date, terminal_id, shift_no) holding the highest revision (R11).
-- No reference to branch_daily_sales: stored with or without the day's summary (R12).
create table branch_shift_close (
    id                bigint        generated always as identity primary key,
    branch_code       varchar(10)   not null references branch (branch_code),
    business_date     date          not null,
    terminal_id       varchar(20)   not null,
    shift_no          integer       not null check (shift_no >= 1),
    revision          integer       not null check (revision >= 1),
    cashier_id        varchar(30),
    opened_at         timestamptz   not null,
    closed_at         timestamptz   not null,
    transaction_count bigint        not null check (transaction_count >= 0),
    total_amount      numeric(14,2) not null check (total_amount >= 0),
    cash_expected     numeric(14,2) not null check (cash_expected >= 0),
    cash_counted      numeric(14,2) not null check (cash_counted >= 0),
    -- Over (positive) or short (negative); derived at HQ, not sent by the branch
    cash_over_short   numeric(14,2) generated always as (cash_counted - cash_expected) stored,
    confirmed_at      timestamptz   not null,
    event_id          uuid          not null,
    received_at       timestamptz   not null default now(),
    constraint uq_branch_shift_close unique (branch_code, business_date, terminal_id, shift_no),
    constraint ck_branch_shift_close_times check (closed_at >= opened_at)
);

-- Tender lines, replaced as a whole when a higher revision arrives. May be empty (a shift with no transactions).
create table branch_shift_close_tender (
    id                    bigint        generated always as identity primary key,
    branch_shift_close_id bigint        not null references branch_shift_close (id) on delete cascade,
    tender_type           varchar(30)   not null references tender_type (tender_type),
    amount                numeric(14,2) not null check (amount >= 0),
    quantity              bigint        not null check (quantity >= 0),
    constraint uq_branch_shift_close_tender unique (branch_shift_close_id, tender_type)
);

-- dead_letter needs no change: record_type holds 'SHIFT_CLOSE' and record_date the business_date.
