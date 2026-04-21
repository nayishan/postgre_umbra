# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires --with-umbra MAP fork'
	unless check_pg_config('^#define USE_UMBRA 1$');

my $node = PostgreSQL::Test::Cluster->new('umbra_truncate');

$node->init();
$node->append_conf(
	'postgresql.conf', qq[
wal_level = 'replica'
autovacuum = off
]);
$node->start();

$node->safe_psql(
	'postgres', q[
CREATE TABLE umbra_trunc(i int);
INSERT INTO umbra_trunc
SELECT generate_series(1, 1000);
CHECKPOINT;
TRUNCATE umbra_trunc;
INSERT INTO umbra_trunc
SELECT generate_series(1, 10);
UPDATE umbra_trunc
SET i = i + 100;
]);

$node->stop('immediate');
ok($node->start(), 'restart after truncate crash');

is($node->safe_psql('postgres',
		'SELECT count(*), sum(i), min(i), max(i) FROM umbra_trunc'),
	'10|1055|101|110',
	'truncate redo preserved only post-truncate rows');

# Exercise normal mapped writes after crash recovery.  The table should no
# longer behave as if its logical size were 0, and follow-up restart should
# keep both the recovered rows and the new rows.
$node->safe_psql(
	'postgres', q[
UPDATE umbra_trunc
SET i = i + 1000
WHERE i <= 105;
INSERT INTO umbra_trunc VALUES (9999);
CHECKPOINT;
]);

$node->stop('immediate');
ok($node->start(), 'restart after post-recovery writes');

is($node->safe_psql('postgres',
		'SELECT count(*), sum(i), min(i), max(i) FROM umbra_trunc'),
	'11|16054|106|9999',
	'post-recovery mapped writes survived second restart');

done_testing();
