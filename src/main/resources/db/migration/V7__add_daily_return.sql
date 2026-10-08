-- Daily returns and voids of a branch (contract DailyReturn): same shape as branch_daily_sales, one row per
-- (branch_code, return_date) holding the highest revision. Stored only when the branch's daily sales of that date
-- exist (reference check PARENT_MISSING); the parent is not a foreign key, so a replaced revision of the sales
-- never cascades into the returns.
create table branch_daily_return (
    id           bigint        generated always as identity primary key,
    branch_code  varchar(10)   not null references branch (branch_code),
    return_date  date          not null,
    revision     integer       not null check (revision >= 1),
    total_amount numeric(14,2) not null check (total_amount >= 0),
    confirmed_at timestamptz   not null,
    event_id     uuid          not null,
    received_at  timestamptz   not null default now(),
    constraint uq_branch_daily_return unique (branch_code, return_date)
);

create table branch_daily_return_line (
    id                     bigint        generated always as identity primary key,
    branch_daily_return_id bigint        not null references branch_daily_return (id) on delete cascade,
    category_code          varchar(30)   not null references category (category_code),
    amount                 numeric(14,2) not null check (amount >= 0),
    quantity               bigint        not null check (quantity >= 0),
    constraint uq_branch_daily_return_line unique (branch_daily_return_id, category_code)
);

-- A rejected record is now identified by its type as well: offsets are per topic.
alter table dead_letter add column record_type varchar(20) not null default 'DAILY_SUMMARY';
-- Business date of the record when it could be read: lets HQ find the returns waiting for a day's sales.
alter table dead_letter add column record_date date;
alter table dead_letter drop constraint uq_dead_letter;
alter table dead_letter add constraint uq_dead_letter unique (branch_code, record_type, source_offset);
create index ix_dead_letter_parent on dead_letter (branch_code, record_date) where reject_reason = 'PARENT_MISSING';
