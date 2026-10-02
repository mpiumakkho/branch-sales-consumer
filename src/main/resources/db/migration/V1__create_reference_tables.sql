-- Reference data owned by HQ. The consumer only reads these tables.

-- Branch registry. A branch is added here when it is onboarded (contract: UNKNOWN_BRANCH otherwise).
create table branch (
    branch_code varchar(10)  primary key,
    name        varchar(200) not null,
    created_at  timestamptz  not null default now()
);

-- Standard product categories. Source: contract/categories.md
create table category (
    category_code varchar(30)  primary key,
    description   varchar(200) not null,
    created_at    timestamptz  not null default now()
);
