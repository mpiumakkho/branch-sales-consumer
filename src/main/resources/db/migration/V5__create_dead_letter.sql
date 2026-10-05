-- Summary records that failed a contract check (contract/README.md), kept unchanged for inspection and replay.
-- To process a row again after fixing the cause at HQ: update dead_letter set replay_requested_at = now() where ...
create table dead_letter (
    id                  bigint      generated always as identity primary key,
    branch_code         varchar(10) not null references branch (branch_code),
    -- offset in the branch's branch-sales.daily-summary topic (one partition); receipts refer to it
    source_offset       bigint      not null,
    record_key          text,
    record_value        bytea,
    reject_reason       varchar(30) not null,
    detail              text        not null,
    rejected_at         timestamptz not null default now(),
    replay_requested_at timestamptz,
    replayed_at         timestamptz,
    -- INSERTED, UPDATED, DUPLICATE, STALE or REJECTED (then reject_reason and detail are from the replay)
    replay_result       varchar(30),
    -- A record read again (HQ restart before the offset commit) is kept once
    constraint uq_dead_letter unique (branch_code, source_offset)
);

create index ix_dead_letter_replay on dead_letter (id) where replay_requested_at is not null;
