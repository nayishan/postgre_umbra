# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires --with-umbra MAP fork'
	unless check_pg_config('^#define USE_UMBRA 1$');

my $node = PostgreSQL::Test::Cluster->new('umbra_remap');

$node->init();
$node->append_conf(
	'postgresql.conf', qq[
wal_level = 'replica'
autovacuum = off
]);
$node->start();

$node->safe_psql(
	'postgres', q[
CREATE TABLE umbra_hash(k int, filler text);
INSERT INTO umbra_hash
SELECT g % 97, repeat(md5(g::text), 2)
FROM generate_series(1, 4000) AS g;
CREATE INDEX umbra_hash_idx ON umbra_hash USING hash (k);

CREATE TABLE umbra_brin(i int, filler text);
INSERT INTO umbra_brin
SELECT g, repeat('x', 200)
FROM generate_series(1, 12000) AS g;
CREATE INDEX umbra_brin_idx
ON umbra_brin USING brin (i) WITH (pages_per_range = 1);
ANALYZE umbra_hash;
ANALYZE umbra_brin;
]);

$node->stop('immediate');
ok($node->start(), 'restart after hash/brin index build crash');

my $hash_plan = $node->safe_psql(
	'postgres', q[
SET enable_seqscan = off;
EXPLAIN (COSTS OFF)
SELECT count(*) FROM umbra_hash WHERE k = 42;
]);
like($hash_plan, qr/umbra_hash_idx/, 'hash index plan survived recovery');

is($node->safe_psql('postgres',
		q[SELECT count(*) FROM umbra_hash WHERE k = 42]),
	'41', 'hash index-backed equality query returns expected rows');

my $brin_plan = $node->safe_psql(
	'postgres', q[
SET enable_seqscan = off;
EXPLAIN (COSTS OFF)
SELECT count(*) FROM umbra_brin WHERE i BETWEEN 2500 AND 2600;
]);
like($brin_plan, qr/umbra_brin_idx/, 'brin index plan survived recovery');

is($node->safe_psql('postgres',
		q[SELECT count(*) FROM umbra_brin WHERE i BETWEEN 2500 AND 2600]),
	'101', 'brin range query returns expected rows after recovery');

$node->safe_psql(
	'postgres', q[
INSERT INTO umbra_hash
SELECT 42, repeat('y', 64)
FROM generate_series(1, 9);
INSERT INTO umbra_brin
SELECT g, repeat('z', 200)
FROM generate_series(12001, 12200) AS g;
CHECKPOINT;
]);

$node->stop('immediate');
ok($node->start(), 'restart after post-recovery indexed writes');

is($node->safe_psql('postgres',
		q[SELECT count(*) FROM umbra_hash WHERE k = 42]),
	'50', 'hash index remains usable after second restart');

is($node->safe_psql('postgres',
		q[SELECT count(*) FROM umbra_brin WHERE i BETWEEN 12100 AND 12150]),
	'51', 'brin index remains usable after second restart');

done_testing();
