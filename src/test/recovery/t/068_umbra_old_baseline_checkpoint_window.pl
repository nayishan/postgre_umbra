#
# Verify that a post-checkpoint remap keeps the old physical page alive long
# enough for crash recovery before the next checkpoint boundary.
#
# The contract under test is:
# - establish a checkpoint
# - modify existing logical pages afterwards, so redo must rely on the old
#   physical page as baseline instead of a new checkpoint image
# - crash before any later checkpoint
# - restart must still recover the updated relation correctly
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
checkpoint_timeout = '30min'
max_wal_size = '4GB'
});
$node->start();

$node->safe_psql('postgres',
	q{CREATE TABLE umb_old_baseline_t(id int PRIMARY KEY, payload text);});

$node->safe_psql(
	'postgres', q{
INSERT INTO umb_old_baseline_t
SELECT g, repeat('a', 700) FROM generate_series(1, 4000) g;
CHECKPOINT;
UPDATE umb_old_baseline_t
SET payload = md5(id::text) || repeat('u', 668)
WHERE id % 2 = 0;
});

my $before = $node->safe_psql(
	'postgres', q{
SELECT count(*) || ',' ||
	   sum(length(payload))::bigint || ',' ||
	   sum((left(payload, 8) = md5(id::text)::text)::int)::bigint
FROM umb_old_baseline_t;
});

$node->stop('immediate');
$node->start();

my $after = $node->safe_psql(
	'postgres', q{
SELECT count(*) || ',' ||
	   sum(length(payload))::bigint || ',' ||
	   sum((left(payload, 8) = md5(id::text)::text)::int)::bigint
FROM umb_old_baseline_t;
});

is($after, $before,
	'post-checkpoint remap survives crash before next checkpoint');

is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM umb_old_baseline_t
		   WHERE id % 2 = 0
			 AND left(payload, 8) = left(md5(id::text), 8);}),
	'2000',
	'even rows were recovered from remap baseline');

is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM umb_old_baseline_t
		   WHERE id % 2 = 1
			 AND payload = repeat('a', 700);}),
	'2000',
	'odd rows kept original payload');

done_testing();
