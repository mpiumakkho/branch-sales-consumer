-- Architecture v2 (requirements §16): every branch runs its own Kafka broker and HQ connects to it.
-- The consumer reads from every branch with an address here; null means HQ does not read from the branch
-- (not onboarded yet, or offboarded). The branch row and its sales stay either way (Q6).
alter table branch add column kafka_bootstrap varchar(255);
