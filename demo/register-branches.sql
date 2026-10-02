-- Demo: register the two demo branches in the HQ branch registry (onboarding step, done by the HQ admin).
-- Messages from a branch that is not registered go to the dead-letter topic (UNKNOWN_BRANCH).
insert into branch (branch_code, name) values
    ('BR0001', 'Demo branch 1'),
    ('BR0002', 'Demo branch 2')
on conflict (branch_code) do nothing;
