-- One row per branch per business day (R2). Holds the highest revision received (R4).
create table branch_daily_sales (
    id           bigint        generated always as identity primary key,
    branch_code  varchar(10)   not null references branch (branch_code),
    sale_date    date          not null,
    revision     integer       not null check (revision >= 1),
    total_amount numeric(14,2) not null check (total_amount >= 0),
    confirmed_at timestamptz   not null,
    -- eventId of the message that produced the stored revision, for tracing back to Kafka and the branch
    event_id     uuid          not null,
    -- when HQ stored this revision
    received_at  timestamptz   not null default now(),
    constraint uq_branch_daily_sales unique (branch_code, sale_date)
);

-- Lines are replaced as a whole when a higher revision arrives.
create table branch_daily_sales_line (
    id                    bigint        generated always as identity primary key,
    branch_daily_sales_id bigint        not null references branch_daily_sales (id) on delete cascade,
    category_code         varchar(30)   not null references category (category_code),
    amount                numeric(14,2) not null check (amount >= 0),
    quantity              bigint        not null check (quantity >= 0),
    constraint uq_branch_daily_sales_line unique (branch_daily_sales_id, category_code)
);
