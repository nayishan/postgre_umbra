# Verify 2PC + remap workload correctness across crash recovery.
#
# This is UMBRA-specific and skipped in md mode.
use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires --with-umbra MAP fork'
	unless check_pg_config('^#define USE_UMBRA 1$');

my $node = PostgreSQL::Test::Cluster->new('master');
$node->init();
$node->append_conf(
	'postgresql.conf', qq{
autovacuum = off
full_page_writes = on
max_prepared_transactions = 10
});
$node->start();

$node->safe_psql('postgres',
	q{CREATE TABLE umb_2pc_t(id int PRIMARY KEY, payload text);});

$node->safe_psql(
	'postgres', q{
CREATE INDEX umb_2pc_payload_idx ON umb_2pc_t ((left(payload, 16)));
INSERT INTO umb_2pc_t
SELECT g, repeat('b', 300) FROM generate_series(1, 15000) g;
CHECKPOINT;
});

$node->safe_psql(
	'postgres', q{
BEGIN;
UPDATE umb_2pc_t SET payload = 'gx1_' || id::text WHERE id % 5 = 0;
DELETE FROM umb_2pc_t WHERE id % 97 = 0;
INSERT INTO umb_2pc_t SELECT g, repeat('x', 300) FROM generate_series(20001, 20500) g;
PREPARE TRANSACTION 'umbra_gx1';
});

$node->safe_psql(
	'postgres', q{
BEGIN;
UPDATE umb_2pc_t
SET payload = 'gx2_' || id::text
WHERE id % 5 = 1 AND id % 97 <> 0;
INSERT INTO umb_2pc_t SELECT g, repeat('y', 300) FROM generate_series(21001, 21200) g;
PREPARE TRANSACTION 'umbra_gx2';
});

$node->stop('immediate');
$node->start();

is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_prepared_xacts WHERE gid IN ('umbra_gx1','umbra_gx2');}),
	'2',
	'prepared transactions survive crash recovery');

$node->safe_psql('postgres', q{COMMIT PREPARED 'umbra_gx1';});
$node->safe_psql('postgres', q{ROLLBACK PREPARED 'umbra_gx2';});

is($node->safe_psql('postgres', q{SELECT count(*) FROM umb_2pc_t;}), '15346',
	'row count matches expected after commit/rollback prepared');
is($node->safe_psql('postgres', q{SELECT count(*) FROM umb_2pc_t WHERE id BETWEEN 20001 AND 20500;}), '500',
	'gx1 inserted rows are visible');
is($node->safe_psql('postgres', q{SELECT count(*) FROM umb_2pc_t WHERE id BETWEEN 21001 AND 21200;}), '0',
	'gx2 inserted rows are absent');
is($node->safe_psql('postgres', q{SELECT count(*) FROM umb_2pc_t WHERE id % 5 = 0 AND payload LIKE 'gx1_%';}), '2970',
	'gx1 updates are visible with expected count');
is($node->safe_psql('postgres', q{SELECT count(*) FROM umb_2pc_t WHERE payload LIKE 'gx2_%';}), '0',
	'gx2 updates are absent after rollback prepared');

my $idx_count = $node->safe_psql(
	'postgres', q{
SET enable_seqscan = off;
SELECT count(*) FROM umb_2pc_t WHERE id BETWEEN 100 AND 14900;
});
my $seq_count = $node->safe_psql(
	'postgres', q{
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM umb_2pc_t WHERE id BETWEEN 100 AND 14900;
});
is($idx_count, $seq_count, 'index path and seq path match after 2PC recovery');

done_testing();
