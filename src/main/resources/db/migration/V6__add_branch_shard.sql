-- Which consumer instance reads a branch. Every consumer instance is started with one shard name (CONSUMER_SHARD)
-- and connects to the branches of that shard only, so the branches can be spread over several instances.
alter table branch add column shard varchar(30) not null default 'default';
